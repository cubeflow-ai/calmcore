use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use croaring::Bitmap;
use itertools::Itertools;
use mem_btree::{persist::TreeReader, BTree};

use crate::{
    analyzer::{Analyzer, Token},
    entity::{ArchivedTermPosition, TermPosition},
    index_store::store::InvertIndexReader,
    util::CoreResult,
};

pub(crate) enum TermPositionReader {
    Memory(BTree<String, Arc<RwLock<TermPosition>>>),
    Disk(Arc<TreeReader<String, &'static ArchivedTermPosition>>),
}

impl TermPositionReader {
    pub fn get(&self, key: &String) -> Option<PositionList> {
        match self {
            TermPositionReader::Memory(tree) => Some(PositionList::Memory(tree.get(key)?.clone())),
            TermPositionReader::Disk(tree) => Some(PositionList::Disk(tree.get(key)?)),
        }
    }

    pub(crate) fn clone_map(&self) -> BTree<String, Arc<RwLock<TermPosition>>> {
        match self {
            TermPositionReader::Memory(tree) => tree.clone(),
            TermPositionReader::Disk(_) => panic!("Disk TermPositionReader cannot clone"),
        }
    }
}

pub enum PositionList {
    Memory(Arc<RwLock<TermPosition>>),
    Disk(&'static ArchivedTermPosition),
}

impl PositionList {
    pub fn len(&self) -> usize {
        match self {
            PositionList::Memory(arc) => arc.read().unwrap().ids.len(),
            PositionList::Disk(archived) => archived.ids.len(),
        }
    }

    // current document count
    fn tf(&self, doc_id: u32) -> usize {
        //TODO impl me

        100
    }

    pub(crate) fn write_map(&self, bitmap: &mut Bitmap) {
        match self {
            PositionList::Memory(rw_lock) => {
                let tp = rw_lock.read().unwrap();
                for id in tp.ids.iter() {
                    bitmap.add(*id);
                }
            }
            PositionList::Disk(archived_term_position) => {
                for id in archived_term_position.ids.iter() {
                    bitmap.add(id.to_native());
                }
            }
        }
    }
}

struct PositionListIter<'a> {
    inner: &'a PositionList,
    index: usize,
    len: usize,
}

impl<'a> PositionListIter<'a> {
    fn new(inner: &'a PositionList) -> Self {
        let len = inner.len();
        Self {
            inner,
            index: 0,
            len,
        }
    }

    fn next(&mut self, id: u32) -> Result<bool, u32> {
        loop {
            if self.index >= self.len {
                return Ok(false);
            }
            let current_id = self.current_id();
            if id > current_id {
                self.index += 1;
                continue;
            } else if id == current_id {
                return Ok(true);
            } else {
                return Err(current_id);
            }
        }
    }

    fn current_id(&self) -> u32 {
        match self.inner {
            PositionList::Memory(arc) => arc.read().unwrap().ids[self.index],
            PositionList::Disk(archived) => archived.ids[self.index].to_native(),
        }
    }

    fn positions(&self) -> Vec<u32> {
        match self.inner {
            PositionList::Memory(t) => {
                let tp = t.read().unwrap();
                let start = tp.index[self.index];
                let end = tp.length[self.index];
                tp.offsets[start as usize..=end as usize].to_vec()
            }
            PositionList::Disk(archived) => {
                let start = archived.index[self.index].to_native();
                let end = archived.length[self.index].to_native();
                archived.offsets[start as usize..=end as usize]
                    .iter()
                    .map(|i| i.to_native())
                    .collect_vec()
            }
        }
    }

    fn find_positions(&self, pre: &[u32], slop: i32) -> Vec<u32> {
        let mut result = vec![];
        match self.inner {
            PositionList::Memory(t) => {
                let tp = t.read().unwrap();
                let start = tp.index[self.index];
                let end = tp.length[self.index];
                let offsets = &tp.offsets[start as usize..=end as usize];

                let mut o = 0;
                let mut p = 0;
                let slop = slop as u32;
                loop {
                    if o >= offsets.len() || p >= pre.len() {
                        break;
                    }
                    let pos = offsets[o];
                    let pre_pos = pre[p];

                    if pos + slop < pre_pos {
                        o += 1;
                    } else if pos > pre_pos + slop {
                        p += 1;
                    } else {
                        result.push(pos);
                        o += 1;
                    }
                }
            }
            PositionList::Disk(archived) => {
                let start = archived.index[self.index].to_native();
                let end = archived.length[self.index].to_native();

                let offsets = &archived.offsets[start as usize..=end as usize];

                let mut o = 0;
                let mut p = 0;
                let slop = slop as u32;
                loop {
                    if o >= offsets.len() || p >= pre.len() {
                        break;
                    }
                    let pos = offsets[o];
                    let pre_pos = pre[p];

                    if pos + slop < pre_pos {
                        o += 1;
                    } else if pos > pre_pos + slop {
                        p += 1;
                    } else {
                        result.push(pos.to_native());
                        o += 1;
                    }
                }
            }
        }
        return result;
    }
}

pub struct FulltextIndexReader {
    pub start: u64,
    pub inner: Arc<proto::core::Field>,
    pub analyzer: Arc<Analyzer>,
    pub(crate) token_index: InvertIndexReader<String, Bitmap>,
    pub(crate) term_position: TermPositionReader,
    // Field information
    pub doc_count: u32,  // total Document count
    pub total_term: u64, // Average document length
}
impl FulltextIndexReader {
    pub(crate) fn analyzer(&self, value: &str) -> CoreResult<Vec<Token>> {
        Ok(self.analyzer.analyzer_query(value))
    }

    pub(crate) fn phrase_tokens(
        &self,
        tokens: &[Token],
        slop: i32,
    ) -> CoreResult<(Vec<u64>, HashMap<String, PositionList>)> {
        let mut result = HashMap::new();

        for token in tokens.iter() {
            let token = &token.name;
            if result.contains_key(token) {
                continue;
            }

            let value = self.term_position.get(token);

            if value.is_none() {
                return Ok((vec![], result));
            }

            if self.token_index.get(token).is_none() {
                return Ok((vec![], result));
            }

            result.insert(token.to_string(), value.unwrap());
        }

        let hits = self.phrase_hits(self.start, tokens, &result, slop)?;

        Ok((hits, result))
    }

    fn phrase_hits(
        &self,
        start: u64,
        tokens: &[Token],
        term_position: &HashMap<String, PositionList>,
        slop: i32,
    ) -> CoreResult<Vec<u64>> {
        assert!(tokens.len() > 1 && slop > 1);

        let mut iters = tokens
            .iter()
            .map(|t| PositionListIter::new(term_position.get(&t.name).unwrap()))
            .collect_vec();

        let len = iters.len();

        let mut max = 0;

        let mut hits = vec![];

        loop {
            for i in 0..len {
                match iters[i].next(max) {
                    Ok(b) => {
                        if !b {
                            return Ok(hits);
                        }
                    }

                    Err(v) => {
                        max = v;
                        break;
                    }
                }
            }

            if let Some(_count) = self.pharse_filter(&iters, slop) {
                hits.push(max as u64 + start);
            }
        }
    }

    pub fn avgdl(&self) -> f32 {
        self.total_term as f32 / (self.doc_count + 1) as f32
    }

    pub(crate) fn tokens(
        &self,
        tokens: &[Token],
    ) -> CoreResult<HashMap<String, Option<PositionList>>> {
        Ok(tokens
            .iter()
            .map(|token| (token.name.clone(), self.term_position.get(&token.name)))
            .collect())
    }

    fn pharse_filter<'a>(&self, iters: &[PositionListIter<'a>], slop: i32) -> Option<usize> {
        let mut pre = iters[0].positions();

        for i in 1..iters.len() - 1 {
            pre = iters[i].find_positions(&pre, slop);
            if pre.len() == 0 {
                break;
            }
        }

        if pre.len() == 0 {
            return None;
        }
        Some(pre.len())
    }

    // pub fn score_count(
    //     &self,
    //     doc_id: u64,
    //     tokens: &[Token],
    //     term_doc: &HashMap<String, usize>,
    //     avgdl: f32,
    // ) -> f32 {
    //     let doc_len = 200; //TODO: get doc len by doc_id

    //     let mut score = 0.0;
    //     for token in tokens {
    //         if let Some(td) = term_doc.get(token.name.as_str()) {
    //             score += self.bm25(
    //                 p.tf((doc_id - self.start) as u32),
    //                 *td,
    //                 self.doc_count,
    //                 doc_len,
    //                 avgdl,
    //             );
    //         }
    //     }

    //     score
    // }

    pub fn score_text(
        &self,
        doc_id: u64,
        tokens: &[Token],
        term_position: &HashMap<String, Option<Arc<PositionList>>>,
        avgdl: f32,
    ) -> f32 {
        let doc_len = 200; //TODO: get doc len by doc_id

        let mut score = 0.0;
        for token in tokens {
            if let Some(Some(p)) = term_position.get(token.name.as_str()) {
                score += self.bm25(
                    p.tf((doc_id - self.start) as u32),
                    p.len(),
                    self.doc_count,
                    doc_len,
                    avgdl,
                );
            }
        }

        score
    }

    pub fn score_phrase(
        &self,
        doc_id: u64,
        tokens: &[Token],
        term_position: &HashMap<String, PositionList>,
        avgdl: f32,
    ) -> f32 {
        let doc_len = 200; //TODO: get doc len by doc_id

        let mut score = 0.0;
        for token in tokens {
            if let Some(p) = term_position.get(&token.name) {
                score += self.bm25(
                    p.tf((doc_id - self.start) as u32),
                    p.len(),
                    self.doc_count,
                    doc_len,
                    avgdl,
                );
            }
        }

        score
    }

    fn bm25(&self, tf: usize, term_doc: usize, doc_count: u32, doc_len: usize, avgdl: f32) -> f32 {
        let tf = tf as f32;
        let n = term_doc as f32;
        let N = doc_count as f32;
        let doc_len = doc_len as f32;

        let k1 = 1.2;
        let b = 0.75;

        let idf = ((N - n + 0.5) / (n + 0.5) + 1.0).ln();
        let tf_norm = (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * (doc_len / avgdl)));
        return idf * tf_norm;
    }

    fn tf_idf(&self, tf: usize, term_doc: usize, doc_count: u32) -> f32 {
        let tf = tf as f32;
        let n = term_doc as f32;
        let N = doc_count as f32;

        let idf = ((N - n + 0.5) / (n + 0.5) + 1.0).ln();
        return tf * idf;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_phrase_filter() {
        // 创建测试数据
        let tokens = vec![
            Token {
                name: "hello".to_string(),
                attrs: None,
                index: 0,
            },
            Token {
                name: "world".to_string(),
                attrs: None,
                index: 1,
            },
        ];

        let mut offset_map = HashMap::new();
        offset_map.insert("hello".to_string(), vec![1_u32, 5_u32, 10_u32]);
        offset_map.insert("world".to_string(), vec![2_u32, 6_u32, 11_u32]);

        // 测试精确短语匹配
        assert!(pharse_filter(&tokens, &offset_map, 0));

        // 测试带有slop的短语匹配
        assert!(pharse_filter(&tokens, &offset_map, 1));

        // 测试不匹配的情况
        let mut bad_offset_map = HashMap::new();
        bad_offset_map.insert("hello".to_string(), vec![1_u32]);
        bad_offset_map.insert("world".to_string(), vec![10_u32]);
        assert!(!pharse_filter(&tokens, &bad_offset_map, 1));
    }
}
