use std::{collections::HashMap, sync::Arc};

use croaring::Bitmap;

use crate::{
    analyzer::{Analyzer, Token},
    index_store::store::InvertIndexReader,
    util::CoreResult,
};
pub struct FulltextIndexReader {
    pub start: u64,
    pub inner: Arc<proto::core::Field>,
    pub analyzer: Arc<Analyzer>,
    pub(crate) token_index: InvertIndexReader<String, Bitmap>,
    pub(crate) doc_index: InvertIndexReader<(u32, String), Vec<u32>>,
    // Field information
    pub doc_count: u32,  // total Document count
    pub total_term: u64, // Average document length
}
impl FulltextIndexReader {
    pub(crate) fn analyzer(&self, value: &str) -> CoreResult<Vec<Token>> {
        Ok(self.analyzer.analyzer_query(value))
    }

    pub(crate) fn tokens(&self, tokens: &[&String]) -> CoreResult<Vec<Bitmap>> {
        Ok(tokens
            .iter()
            .map(|token| self.token_index.get(token).unwrap_or_default())
            .collect())
    }

    pub(crate) fn score(
        &self,
        doc_id: u32,
        tokens: &[Token],
        token_doc_len: &HashMap<String, usize>,
        operator: bool,
        slop: i32,
    ) -> Option<f32> {
        let offset_map = token_doc_len
            .iter()
            .map(|(token, _)| {
                let key = (doc_id, token.clone());
                let offsets = self.doc_index.get(&key).unwrap_or_default();
                (key.1, offsets)
            })
            .collect::<HashMap<_, _>>();

        // Get document length
        let dl = self
            .doc_index
            .get(&(doc_id, "".to_string()))
            .and_then(|a| a.first().cloned())
            .unwrap_or(1);

        //phrase query filter, if not match return None
        if operator && !pharse_filter(tokens, &offset_map, slop) {
            return None;
        }

        let avgdl = self.total_term as f32 / self.doc_count as f32;

        Some(self.score_bm25(dl as f32, avgdl, token_doc_len, &offset_map))
    }

    // [ \text{Score}(D,Q) = \sum_{i=1}^{n} IDF(q_i) \cdot \frac{f(q_i, D) \cdot (k1 + 1)}{f(q_i, D) + k1 \cdot (1 - b + b \cdot \frac{|D|}{\text{AVGDL}})} ]
    /// BM25 term score calculation
    ///
    fn score_bm25(
        &self,
        dl: f32,
        avgdl: f32,
        token_doc_len: &HashMap<String, usize>, // Token count in store
        offset_map: &HashMap<String, Vec<u32>>, // Token offset map
    ) -> f32 {
        // BM25 参数，可以根据需要调整
        const K1: f32 = 1.2;
        const B: f32 = 0.75;

        let mut score = 0.0;

        for (term, positions) in offset_map {
            // 计算词频 TF
            let tf = positions.len() as f32;

            // 获取文档频率 DF 并计算 IDF
            let df = *token_doc_len.get(term).unwrap_or(&1) as f32;
            let idf = ((self.doc_count as f32 - df + 0.5) / (df + 0.5)).ln();

            // 文档长度归一化
            let norm = 1.0 - B + B * (dl / avgdl);

            // BM25 评分公式
            let term_score = idf * (tf * (K1 + 1.0)) / (tf + K1 * norm);
            score += term_score;
        }

        score
    }
}

/// Phrase query filter function
/// Returns true if the tokens match the phrase query requirements within the given slop distance
fn pharse_filter(tokens: &[Token], offset_map: &HashMap<String, Vec<u32>>, slop: i32) -> bool {
    // If not a phrase query (slop < 0) or only one token, return true directly
    if slop < 0 || tokens.len() <= 1 {
        return true;
    }

    // Quick check: ensure all tokens have position information
    if !tokens.iter().all(|t| offset_map.contains_key(&t.name)) {
        return false;
    }

    // Get position list of the first token
    let first_positions = offset_map.get(&tokens[0].name).unwrap();
    if first_positions.is_empty() {
        return false;
    }

    // Check each position of the first token
    'outer: for &start_pos in first_positions {
        let mut last_pos = start_pos;

        // Check if subsequent tokens can find positions meeting distance requirements
        for window in tokens.windows(2) {
            let current_token = &window[0];
            let next_token = &window[1];

            // Calculate expected position range
            let expected_pos = last_pos as i32 + (next_token.index - current_token.index) as i32;
            let min_pos = expected_pos - slop;
            let max_pos = expected_pos + slop;

            // Find position in next token's position list that meets conditions
            if let Some(&actual_pos) =
                offset_map
                    .get(&next_token.name)
                    .unwrap()
                    .iter()
                    .find(|&&pos| {
                        let pos = pos as i32;
                        pos >= min_pos && pos <= max_pos
                    })
            {
                last_pos = actual_pos;
            } else {
                continue 'outer;
            }
        }

        return true;
    }

    false
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
