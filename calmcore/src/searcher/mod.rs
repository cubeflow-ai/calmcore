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
use half::vec;
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
    filter: Option<Bitmap>,
    stream: Option<Box<dyn HitStream>>,
    segment: &'a SegmentReader,
}

type Streams = Vec<Box<dyn HitStream>>;
type Filters = Vec<Option<Bitmap>>;

pub struct Searcher {
    segments: Vec<SegmentReader>,
}

impl Searcher {
    pub fn new(mut segments: Vec<SegmentReader>) -> Self {
        segments.retain(|s| !s.is_empty());
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
        let sc = SearchContext::new(query, &order_by);

        let result = {
            let (streams, filters) = self.query_execute(query, &sc)?;

            // statistics total hits
            let mut total_hits = filters
                .iter()
                .map(|f| f.as_ref().map(|v| v.cardinality()).unwrap_or(0) as u64)
                .sum::<u64>();

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
                .filter(|(_, f)| match f {
                    Some(b) => b.cardinality() > 0,
                    None => true,
                })
                .map(|(index, filter)| SegmentSearcher {
                    stream: streams.next(),
                    segment: &self.segments[index],
                    filter,
                })
                .collect_vec();

            let (hits, realcount) = self.topn(&sc, projection, searchers, &order_by, limit)?;

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

                let filters = if sc.need_filter {
                    plans
                        .par_iter()
                        .zip(&self.segments)
                        .map(|(p, s)| {
                            let guard = sc.get(s.start());
                            Some(p.as_filter(&guard))
                        })
                        .collect::<Vec<_>>()
                } else {
                    vec![None; plans.len()]
                };

                let streams = if sc.need_stream {
                    plans
                        .into_par_iter()
                        .zip(self.segments.par_iter())
                        .zip(filters.par_iter())
                        .map(|((p, s), f)| {
                            let mut guard = sc.get(s.start());
                            p.into_stream(s.start(), &mut guard, f.as_ref())
                        })
                        .collect::<CoreResult<Vec<_>>>()?
                } else {
                    vec![]
                };

                (streams, filters)
            }
            None => {
                let filters = self
                    .segments
                    .par_iter()
                    .map(|s| Some(s.all_record()))
                    .collect();
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
        sc: &SearchContext,
        projection: Option<&[String]>,
        searches: Vec<SegmentSearcher<'_>>,
        order_by: &Vec<(Arc<Field>, bool)>,
        limit: (usize, usize),
    ) -> CoreResult<(Vec<SortedHit>, Option<u64>)> {
        let size = limit.0 + limit.1;

        if size == 0 {
            return Ok((vec![], None));
        }

        let mut heap = BTreeSet::new();

        let mut list = Vec::with_capacity(size);

        let mut real_count = 0;

        let need_score = order_by
            .iter()
            .any(|(f, _)| f.name.eq_ignore_ascii_case("_score"));

        log::debug!(
            "topn: search_context:{:?}, need_score:{:?}, order_by:{:?}",
            sc,
            need_score,
            order_by
        );

        let mut process_to_heap = |records: Vec<Cow<'_, ObjectValue>>,
                                   ids: &[u64],
                                   scores: &[f32],
                                   min: &mut Option<SortedHit>|
         -> CoreResult<()> {
            real_count += ids.len() as u64;

            for (i, (value, id)) in records
                .into_iter()
                .map(Cow::into_owned)
                .zip(ids)
                .enumerate()
            {
                let score = scores.get(i).cloned().unwrap_or(0.0);

                let sort = SortedHit::make_sort(*id, score, &value, order_by)?;

                if min
                    .as_ref()
                    .map(|m| m.cmp_record(&sort) == Ordering::Less)
                    .unwrap_or(false)
                {
                    continue;
                }

                let sort_hit: SortedHit = SortedHit::new(*id, score, value, sort);

                heap.insert(sort_hit);

                if heap.len() > size {
                    *min = heap.pop_last();
                }
            }
            Ok(())
        };
        let mut min: Option<SortedHit> = None;

        'outer: for search in searches {
            let SegmentSearcher {
                stream,
                filter,
                segment,
            } = search;

            let start = segment.start();

            let mut next = None;

            if let Some(mut stream) = stream {
                let mut ids = Vec::with_capacity(size);
                let mut scores = Vec::with_capacity(size);
                while let Some(id) = stream.next_value(next) {
                    ids.push(id);
                    if need_score {
                        scores.push(stream.score());
                    }
                    next = Some(id + 1);
                    if ids.len() >= 1000 {
                        let records: Vec<Cow<'_, ObjectValue>> =
                            segment.batch_doc(projection, &ids)?;
                        process_to_heap(records, &ids, &scores, &mut min)?;
                        ids.clear();
                        scores.clear();
                    }
                }

                if !ids.is_empty() {
                    let records: Vec<Cow<'_, ObjectValue>> = segment.batch_doc(projection, &ids)?;
                    process_to_heap(records, &ids, &scores, &mut min)?;
                }
            } else {
                let ids = filter
                    .unwrap()
                    .iter()
                    .take(size)
                    .map(|i| i as u64 + start)
                    .collect_vec();

                if !ids.is_empty() {
                    let records: Vec<Cow<'_, ObjectValue>> = segment.batch_doc(projection, &ids)?;
                    for (value, id) in records.into_iter().map(Cow::into_owned).zip(ids) {
                        let sort_hit = SortedHit::new(id, 0.0, value, vec![]);

                        list.push(sort_hit);

                        if list.len() >= size {
                            break 'outer;
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
