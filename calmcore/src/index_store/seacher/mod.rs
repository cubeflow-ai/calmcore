pub(crate) mod context;
pub(crate) mod plan;

use std::{
    borrow::Cow,
    cmp::Ordering,
    collections::{BTreeSet, HashMap},
    sync::{Arc, LazyLock},
};

use context::SearchContext;
use croaring::Bitmap;
use itertools::Itertools;
use plan::{PhysicsPlan, Query};
use proto::core::{
    field::{self},
    Field, Hit, QueryResult, Record,
};
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};

use crate::util::{self, CoreError, CoreResult};

use super::{segment::SegmentReader, stream::HitStream};

static SCORE_FIELD: LazyLock<Arc<Field>> = LazyLock::new(|| {
    Arc::new(Field {
        name: String::from("_score"),
        r#type: field::Type::Float as i32,
        option: None,
    })
});

pub struct SegmentSearcher<'a> {
    stream: Box<dyn HitStream>,
    segment: &'a SegmentReader,
}

impl SegmentSearcher<'_> {
    pub fn next(&mut self) -> Option<Hit> {
        self.stream.next();

        self.stream.value().map(|id| Hit {
            id,
            score: self.stream.score(),
            record: None,
            sort: vec![],
        })
    }

    #[allow(dead_code)]
    pub fn batch_next(&mut self, size: usize) -> Vec<Hit> {
        let mut hits = Vec::with_capacity(size);
        loop {
            if hits.len() >= size {
                break;
            }

            if let Some(hit) = self.next() {
                hits.push(hit);
            } else {
                break;
            }
        }
        hits
    }

    fn doc(&self, id: u64) -> Option<Cow<Record>> {
        self.segment.doc(id)
    }

    #[allow(dead_code)]
    fn batch_doc(&self, ids: &[u64]) -> Vec<Option<Cow<Record>>> {
        self.segment.batch_doc(ids)
    }
}

type Streams = Vec<Box<dyn HitStream>>;
type Filters = Vec<Bitmap>;

pub struct Searcher {
    segments: Vec<SegmentReader>,
}

impl Searcher {
    pub fn new(mut segments: Vec<SegmentReader>) -> Self {
        segments.sort_by_key(|r| std::cmp::Reverse(r.start()));
        Self { segments }
    }

    pub fn search_query(&self, query: Query) -> CoreResult<QueryResult> {
        log::debug!("search_query:{:?}", query);
        if let Query::Search {
            projection,
            query,
            order_by,
            limit,
        } = query
        {
            self.search(
                projection,
                query.as_ref().map(|q| q.as_ref()),
                order_by,
                limit,
            )
        } else {
            Err(CoreError::InvalidParam(format!(
                "query is not a valid query:{:?}",
                query
            )))
        }
    }

    pub fn search(
        &self,
        projection: Vec<String>,
        query: Option<&Query>,
        order_by: Vec<(String, bool)>,
        limit: (usize, usize),
    ) -> CoreResult<QueryResult> {
        let sc = SearchContext::new(&self.segments);

        let result = {
            let (streams, filters) = self.query_execute(query, &sc)?;

            // statistics total hits
            let mut total_hits = filters.iter().map(|f| f.cardinality()).sum::<u64>();

            let order_by = self.make_order_by(order_by)?;

            let (hits, realcount) = if streams.is_empty() && order_by.is_empty() {
                (self.topn_with_filter(limit, filters)?, None)
            } else {
                self.topn(limit, &order_by, streams)?
            };

            if let Some(realcount) = realcount {
                total_hits = realcount;
            }

            let hits = self.projection(&projection, hits)?;

            QueryResult { hits, total_hits }
        };

        Ok(result)
    }

    fn query_execute(
        &self,
        query: Option<&Query>,
        sc: &SearchContext,
    ) -> CoreResult<(Streams, Filters)> {
        let value = match query {
            Some(query) => {
                let plans = self
                    .segments
                    .par_iter()
                    .map(|s| {
                        let mut guard = sc.get(s.start()).unwrap().lock().unwrap();
                        PhysicsPlan::new(s, query, &mut guard)
                    })
                    .collect::<CoreResult<Vec<PhysicsPlan>>>()?;
                let filters = plans
                    .par_iter()
                    .zip(&self.segments)
                    .map(|(p, s)| {
                        let guard = sc.get(s.start()).unwrap().lock().unwrap();
                        p.as_filter(&guard)
                    })
                    .collect::<Vec<_>>();

                let streams = plans
                    .into_par_iter()
                    .zip(self.segments.par_iter())
                    .map(|(p, s)| (p, s))
                    .zip(filters.par_iter())
                    .map(|((p, s), f)| {
                        let mut guard = sc.get(s.start()).unwrap().lock().unwrap();
                        p.into_stream(s.start(), &mut guard, f)
                    })
                    .collect::<Vec<_>>();
                (streams, filters)
            }
            None => {
                let filters = self.segments.par_iter().map(|s| s.all_record()).collect();
                (vec![], filters)
            }
        };
        Ok(value)
    }

    fn projection(&self, projection: &Vec<String>, hits: Vec<SortedHit>) -> CoreResult<Vec<Hit>> {
        let mut result = Vec::with_capacity(hits.len());
        for hit in hits {
            let SortedHit {
                id,
                score,
                mut record,
                value,
                sort,
                ..
            } = hit;

            if !projection.is_empty() {
                let mut new_obj = HashMap::new();
                if let serde_json::Value::Object(mut data) = value {
                    for field in projection {
                        if let Some(v) = data.remove(field) {
                            new_obj.insert(field, v);
                        }
                    }
                    if let Ok(v) = serde_json::to_vec(&new_obj) {
                        record.data = v;
                    }
                }
            }

            result.push(Hit {
                id,
                score,
                record: Some(record),
                sort,
            });
        }

        Ok(result)
    }

    fn make_order_by(&self, order_by: Vec<(String, bool)>) -> CoreResult<Vec<(Arc<Field>, bool)>> {
        order_by
            .into_iter()
            .map(|(field, asc)| {
                let field = if field.eq_ignore_ascii_case("_score") {
                    SCORE_FIELD.clone()
                } else {
                    match self.segments[0].get_field(&field) {
                        Some(f) => f,
                        None => {
                            return Err(CoreError::InvalidParam(format!(
                                "field:{} not found",
                                field
                            )))
                        }
                    }
                };
                Ok((field, asc))
            })
            .collect::<CoreResult<Vec<(Arc<Field>, bool)>>>()
    }

    fn topn_with_filter(
        &self,
        limit: (usize, usize),
        filters: Vec<Bitmap>,
    ) -> CoreResult<Vec<SortedHit>> {
        let size = limit.0 + limit.1;

        let mut skip = limit.0 as i32;

        let mut results = Vec::with_capacity(size);

        'outer: for (b, s) in filters.into_iter().zip(self.segments.iter()) {
            for v in b.iter() {
                if skip > 0 {
                    skip -= 1;
                    continue;
                }

                let id = v as u64 + s.start();

                if let Some(record) = s.doc(id) {
                    results.push(SortedHit {
                        id,
                        score: 0.0,
                        record: record.into_owned(),
                        value: serde_json::Value::Null,
                        sort: Vec::new(),
                    });
                }

                if results.len() >= size {
                    break 'outer;
                }
            }
        }

        Ok(results.into_iter().skip(limit.0).collect_vec())
    }
}

#[derive(Debug)]
struct SortedHit {
    id: u64,
    score: f32,
    record: Record,
    value: serde_json::Value,
    sort: Vec<Vec<u8>>,
}

impl SortedHit {
    fn new(hit: Hit, record: Record, value: serde_json::Value, sort: Vec<Vec<u8>>) -> Self {
        Self {
            id: hit.id,
            score: hit.score,
            record,
            value,
            sort,
        }
    }

    fn cmp_record(&self, sort: &Vec<Vec<u8>>) -> Ordering {
        for (a, b) in self.sort.iter().zip(sort) {
            match a.cmp(b) {
                Ordering::Equal => continue,
                ord => return ord,
            }
        }
        Ordering::Equal
    }

    fn make_sort(
        id: u64,
        score: f32,
        data: &[u8],
        order_by: &Vec<(Arc<Field>, bool)>,
    ) -> CoreResult<(serde_json::Value, Vec<Vec<u8>>)> {
        let data = serde_json::from_slice::<serde_json::Value>(data)?;

        let encode_field =
            |tp: &field::Type, value: Option<&serde_json::Value>| -> CoreResult<Vec<u8>> {
                let value = match value {
                    Some(v) => v,
                    None => return Ok(vec![]),
                };
                let value = util::json_value_to_string(value);

                util::str_to_vec_fix_type(&value, tp)
            };

        let mut sort = Vec::with_capacity(order_by.len() + 1);

        for (field, asc) in order_by {
            match field.name.as_str() {
                "_score" => {
                    let mut vec = memcomparable::to_vec(&score)
                        .map_err(|e| CoreError::Internal(e.to_string()))?;
                    if !*asc {
                        for i in vec.iter_mut() {
                            *i = 255 - *i;
                        }
                    }
                    sort.push(vec);
                }
                _ => sort.push(encode_field(&field.r#type(), data.get(&field.name))?),
            }
        }

        sort.push(id.to_be_bytes().to_vec());

        Ok((data, sort))
    }
}

impl PartialOrd for SortedHit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortedHit {
    fn cmp(&self, other: &Self) -> Ordering {
        self.sort.cmp(&other.sort)
    }
}

impl PartialEq for SortedHit {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.score == other.score && self.sort == other.sort
    }
}

impl Eq for SortedHit {}

impl Searcher {
    fn topn(
        &self,
        limit: (usize, usize),
        order_by: &Vec<(Arc<Field>, bool)>,
        streams: Vec<Box<dyn HitStream>>,
    ) -> CoreResult<(Vec<SortedHit>, Option<u64>)> {
        let streams = self
            .segments
            .par_iter()
            .zip(streams)
            .map(|(segment, stream)| SegmentSearcher { stream, segment })
            .collect::<Vec<SegmentSearcher>>();

        let size = limit.0 + limit.1;

        let mut heap = BTreeSet::new();

        let mut real_count: u64 = 0;

        for mut stream in streams {
            let mut min: Option<SortedHit> = None;

            // loop {
            //     let hits = stream.batch_next(size);
            //     let hit_size = hits.len();
            //     let ids = hits.iter().map(|h| h.id).collect_vec();

            //     for (record, hit) in stream.batch_doc(&ids).into_iter().zip(hits) {
            //         let record = match record {
            //             Some(record) => record,
            //             None => continue,
            //         };

            //         real_count += 1;

            //         let (value, sort) =
            //             SortedHit::make_sort(hit.id, hit.score, &record.data, order_by)?;

            //         let sort_hit = if min.is_none()
            //             || min.as_ref().unwrap().cmp_record(&sort) == Ordering::Greater
            //         {
            //             SortedHit::new(hit, record.into_owned(), value, sort)
            //         } else {
            //             continue;
            //         };

            //         heap.insert(sort_hit);

            //         if heap.len() > size {
            //             min = heap.pop_last();
            //             //if order by only one field, it is id ,so we can return early
            //             if order_by.is_empty() {
            //                 return Ok((
            //                     heap.into_iter().skip(limit.0).take(limit.1).collect_vec(),
            //                     None,
            //                 ));
            //             }
            //         }
            //     }
            //     if hit_size < size {
            //         break;
            //     }
            // }

            while let Some(hit) = stream.next() {
                let record = match stream.doc(hit.id) {
                    Some(record) => record,
                    None => continue,
                };

                real_count += 1;

                let (value, sort) =
                    SortedHit::make_sort(hit.id, hit.score, &record.data, order_by)?;

                let sort_hit = if min.is_none()
                    || min.as_ref().unwrap().cmp_record(&sort) == Ordering::Greater
                {
                    SortedHit::new(hit, (*record).clone(), value, sort)
                } else {
                    continue;
                };

                heap.insert(sort_hit);

                if heap.len() > size {
                    min = heap.pop_last();
                    //if order by only one field, it is id ,so we can return early
                    if order_by.is_empty() {
                        return Ok((
                            heap.into_iter().skip(limit.0).take(limit.1).collect_vec(),
                            None,
                        ));
                    }
                }
            }
        }

        Ok((
            heap.into_iter().skip(limit.0).take(limit.1).collect_vec(),
            Some(real_count),
        ))
    }
}
