use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use itertools::Itertools;
use proto::core::field::{fulltext_option, FulltextOption};
use rust_stemmers::Stemmer;

use crate::util::CoreResult;

mod dict;
mod tokenizer_standard;
mod tokenizer_whitespace;

#[derive(Debug, Clone)]
pub struct TokenAttr {}

#[derive(Debug, Clone)]
pub struct Token {
    pub name: String,
    pub attrs: Option<Vec<TokenAttr>>,
    pub index: usize,
}

impl Token {
    pub fn new(name: &str) -> Token {
        Token {
            name: name.to_string(),
            attrs: None,
            index: 0,
        }
    }
}

trait Tokenizer {
    fn tokenize(&self, text: &str) -> Vec<Token>;
}

pub struct Analyzer {
    tokenizer: Box<dyn Tokenizer + Send + Sync + 'static>,
    lowercase: bool,
    stopwords: HashSet<String>,
    synonyms: HashMap<String, Arc<Vec<String>>>,
    stemmer: Option<Stemmer>,
}

impl Analyzer {
    pub(crate) fn default() -> Analyzer {
        Self {
            tokenizer: tokenizer_standard::StandardTokenizer::instance(),
            lowercase: true,
            stemmer: None,
            stopwords: HashSet::new(),
            synonyms: HashMap::new(),
        }
    }

    pub fn new(op: &FulltextOption) -> CoreResult<Self> {
        let tokenizer = match op.tokenizer() {
            fulltext_option::Tokenizer::Standard => {
                tokenizer_standard::StandardTokenizer::instance()
            }
            fulltext_option::Tokenizer::Whitespace => {
                tokenizer_whitespace::WhitespaceTokenizer::instance()
            }
        };

        let lowercase = op
            .filters
            .iter()
            .contains(&(fulltext_option::Filter::Lowercase as i32));

        let stemmer = if op
            .filters
            .iter()
            .contains(&(fulltext_option::Filter::Stemmer as i32))
        {
            Some(Stemmer::create(rust_stemmers::Algorithm::English))
        } else {
            None
        };

        let stopwords = dict::stopwords(op)?;

        let synonyms = dict::synonyms(op)?;

        Ok(Self {
            tokenizer,
            lowercase,
            stemmer,
            stopwords,
            synonyms,
        })
    }

    pub fn analyzer_index(&self, text: &str) -> Vec<Token> {
        let result = if self.lowercase {
            self.tokenizer.tokenize(&text.to_lowercase())
        } else {
            self.tokenizer.tokenize(text)
        };

        result
            .into_iter()
            .filter(|t| !self.stopwords.contains(&t.name))
            .enumerate()
            .map(|(i, mut t)| {
                if let Some(stemmer) = &self.stemmer {
                    let new_str = stemmer.stem(&t.name);
                    if t.name != new_str {
                        t.name = new_str.to_string();
                    }
                }
                t.index = i;
                t
            })
            .collect()
    }

    pub fn analyzer_query(&self, text: &str) -> Vec<Token> {
        let tokens = self.tokenizer.tokenize(text);
        let mut result = Vec::with_capacity(tokens.len());
        for (i, mut t) in tokens
            .into_iter()
            .filter(|t| !self.stopwords.contains(&t.name))
            .enumerate()
        {
            if let Some(synonyms) = self.synonyms.get(&t.name) {
                for s in synonyms.iter() {
                    let mut t = t.clone();
                    if let Some(stemmer) = &self.stemmer {
                        let new_str = stemmer.stem(&t.name);
                        if t.name != new_str {
                            t.name = new_str.to_string();
                        }
                    } else {
                        t.name.clone_from(s);
                    }
                    t.index = i;
                    result.push(t);
                }
            } else {
                if let Some(stemmer) = &self.stemmer {
                    let new_str = stemmer.stem(&t.name);
                    if t.name != new_str {
                        t.name = new_str.to_string();
                    }
                }
                t.index = i;
            }

            result.push(t);
        }

        result
    }
}
