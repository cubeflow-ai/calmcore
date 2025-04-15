pub(crate) mod context;
pub(crate) mod plan;

use std::{
    borrow::Cow,
    cmp::Ordering,
    collections::BTreeSet,
    sync::{Arc, LazyLock},
};

use context::SearchContext;
use croaring::Bitmap;
use itertools::Itertools;
use plan::{PhysicsPlan, Query};
use proto::core::{
    field::{self},
    Field, Hit, ObjectValue, QueryResult,
};
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};

use crate::{
    index_store::{segment::SegmentReader, stream::HitStream},
    util::{self, kind_to_vec, CoreError, CoreResult},
};

static SCORE_FIELD: LazyLock<Arc<Field>> = LazyLock::new(|| {
    Arc::new(Field {
        name: String::from("_score"),
        r#type: field::Type::Float as i32,
        option: None,
    })
});

pub struct SegmentSearcher<'a> {
    filter: Bitmap,
    stream: Option<Box<dyn HitStream>>,
    segment: &'a SegmentReader,
}

impl SegmentSearcher<'_> {
    fn batch_doc(
        &self,
        columns: Option<&[String]>,
        ids: &[u64],
    ) -> CoreResult<Vec<Cow<ObjectValue>>> {
        self.segment.batch_doc(columns, ids)
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
        let sc = SearchContext::new();

        let result = {
            let (streams, filters) = self.query_execute(query, &sc)?;

            // statistics total hits
            let mut total_hits = filters.iter().map(|f| f.cardinality()).sum::<u64>();

            let order_by = self.make_order_by(order_by)?;

            let projection = if projection.len() == 0 {
                None
            } else {
                Some(projection.as_slice())
            };

            let mut streams = streams.into_iter();

            let searchers: Vec<SegmentSearcher<'_>> = filters
                .into_iter()
                .enumerate()
                .map(|(index, filter)| SegmentSearcher {
                    stream: streams.next(),
                    segment: &self.segments[index],
                    filter,
                })
                .collect_vec();

            let need_realcount = query.map(|q| q.need_realcount()).unwrap_or_default();
            let (hits, realcount) =
                self.topn(projection, searchers, &order_by, limit, need_realcount)?;
            if let Some(realcount) = realcount {
                total_hits = realcount;
            }

            let hits = self.projection(hits)?;

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
                        let mut guard = sc.get(s.start());
                        PhysicsPlan::new(s, query, &mut guard)
                    })
                    .collect::<CoreResult<Vec<PhysicsPlan>>>()?;
                let filters = plans
                    .par_iter()
                    .zip(&self.segments)
                    .map(|(p, s)| {
                        let guard = sc.get(s.start());
                        p.as_filter(&guard)
                    })
                    .collect::<Vec<_>>();

                let streams = plans
                    .into_par_iter()
                    .zip(self.segments.par_iter())
                    .map(|(p, s)| (p, s))
                    .zip(filters.par_iter())
                    .map(|((p, s), f)| {
                        let mut guard = sc.get(s.start());
                        p.into_stream(s.start(), &mut guard, f)
                    })
                    .collect::<CoreResult<Vec<_>>>()?;
                (streams, filters)
            }
            None => {
                let filters = self.segments.par_iter().map(|s| s.all_record()).collect();
                (vec![], filters)
            }
        };
        Ok(value)
    }

    fn projection(&self, hits: Vec<SortedHit>) -> CoreResult<Vec<Hit>> {
        let mut result = Vec::with_capacity(hits.len());
        for hit in hits {
            let SortedHit {
                id,
                score,
                value,
                sort,
                ..
            } = hit;

            result.push(Hit {
                id,
                score,
                value: Some(value),
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
}

#[derive(Debug)]
struct SortedHit {
    id: u64,
    score: f32,
    value: ObjectValue,
    sort: Vec<Vec<u8>>,
}

impl SortedHit {
    fn new(id: u64, score: f32, value: ObjectValue, sort: Vec<Vec<u8>>) -> Self {
        Self {
            id,
            score,
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
        obj: &ObjectValue,
        order_by: &Vec<(Arc<Field>, bool)>,
    ) -> CoreResult<Vec<Vec<u8>>> {
        let encode_field = |value: Option<&proto::core::Value>| -> CoreResult<Vec<u8>> {
            match value.and_then(|v| v.kind.as_ref()) {
                Some(k) => match kind_to_vec(k)? {
                    util::KindType::Single(v) => Ok(v),
                    util::KindType::Array(_) => unreachable!(),
                },
                None => Ok(vec![]),
            }
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
                _ => sort.push(encode_field(obj.fields.get(&field.name))?),
            }
        }

        sort.push(id.to_be_bytes().to_vec());

        Ok(sort)
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
        projection: Option<&[String]>,
        searches: Vec<SegmentSearcher<'_>>,
        order_by: &Vec<(Arc<Field>, bool)>,
        limit: (usize, usize),
        need_realcount: bool,
    ) -> CoreResult<(Vec<SortedHit>, Option<u64>)> {
        let size = limit.0 + limit.1;

        if size == 0 {
            return Ok((vec![], None));
        }

        let mut heap = BTreeSet::new();
        let mut list = vec![];

        let mut real_count = 0;

        let need_all = !order_by.is_empty() || need_realcount;
        let need_score = order_by
            .iter()
            .any(|(f, _)| f.name.eq_ignore_ascii_case("_score"));

        for search in searches {
            let stream = &search.stream;
            let filter = &search.filter;
            let start = search.segment.start();
            let mut min: Option<SortedHit> = None;

            for ids in &filter.iter().chunks(size) {
                let ids = ids.into_iter().map(|id| id as u64 + start).collect_vec();

                let records = search.batch_doc(projection, &ids)?;

                if need_all {
                    for (value, id) in records.into_iter().map(Cow::into_owned).zip(ids) {
                        real_count += 1;

                        let score = if need_score {
                            match stream.as_ref().and_then(|s| s.score(id)) {
                                Some(s) => s,
                                None => continue,
                            }
                        } else {
                            0.0
                        };
                        let sort = SortedHit::make_sort(id, 0.0, &value, order_by)?;

                        let sort_hit = if min.is_none()
                            || min.as_ref().unwrap().cmp_record(&sort) == Ordering::Greater
                        {
                            SortedHit::new(id, score, value, sort)
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
                } else {
                    for (value, id) in records.into_iter().map(Cow::into_owned).zip(ids) {
                        let sort_hit = SortedHit::new(id, 0.0, value, vec![]);

                        list.push(sort_hit);

                        if list.len() >= size {
                            break;
                        }
                    }
                }
            }
        }

        if !list.is_empty() {
            return Ok((
                if limit.0 == 0 && list.len() == size {
                    list
                } else {
                    list.into_iter().skip(limit.0).take(limit.1).collect_vec()
                },
                None,
            ));
        }

        Ok((
            heap.into_iter().skip(limit.0).take(limit.1).collect_vec(),
            Some(real_count as u64),
        ))
    }
}
