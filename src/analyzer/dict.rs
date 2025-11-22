use std::{
    collections::{HashMap, HashSet},
    io::BufRead,
    sync::Arc,
};

use proto;
use proto::core::field::FulltextOption;
use itertools::Itertools;

use crate::util::{CoreError, CoreResult};

pub fn stopwords(f: &FulltextOption) -> CoreResult<HashSet<String>> {
    if let Some(dict) = &f.stopwords {
        load(dict)
    } else {
        Ok(Default::default())
    }
}

pub fn synonyms(f: &FulltextOption) -> CoreResult<HashMap<String, Arc<Vec<String>>>> {
    let mut result = HashMap::default();
    if let Some(dict) = &f.synonyms {
        let lines = load(dict)?;
        for line in lines {
            let value = Arc::new(line.split('\t').map(ToString::to_string).collect_vec());

            for v in value.iter() {
                result.insert(v.clone(), value.clone());
            }
        }
    }
    Ok(result)
}

fn load(dict: &proto::core::Dict) -> CoreResult<HashSet<String>> {
    match dict.protocol() {
        proto::core::dict::Protocol::Json => {
            let arr: serde_json::Value = serde_json::from_str(&dict.value).map_err(|e| {
                CoreError::InvalidParam(format!(
                    "dict:{:?} can to json array err:{:?}",
                    dict.name, e
                ))
            })?;

            if let serde_json::Value::Array(arr) = arr {
                let result: CoreResult<HashSet<String>> = arr
                    .into_iter()
                    .map(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .ok_or(CoreError::InvalidParam(format!(
                                "dict:{:?} value:{:?} is not string",
                                dict.name, v,
                            )))
                    })
                    .collect();
                Ok(result?)
            } else {
                Err(CoreError::InvalidParam(format!(
                    "dict:{:?} is not json array",
                    dict.name
                )))
            }
        }
        proto::core::dict::Protocol::Api => {
            let response = reqwest::blocking::get(&dict.value).map_err(|e| {
                CoreError::InvalidParam(format!(
                    "dict:{:?} url:{:?} can not read err:{:?}",
                    dict.name, dict.value, e
                ))
            })?;
            Ok(response
                .text()
                .map_err(|e| {
                    CoreError::InvalidParam(format!(
                        "dict:{:?} value not text err:{:?}",
                        dict.name, e
                    ))
                })?
                .split('\n')
                .map(|l| l.to_string())
                .collect())
        }
        proto::core::dict::Protocol::File => {
            let file = std::fs::File::open(&dict.value).map_err(|_e| {
                CoreError::InvalidParam(format!("dict:{:?} can't open file", dict.name))
            })?;
            let reader = std::io::BufReader::new(file);
            let lines: Result<Vec<String>, _> = reader.lines().collect();
            match lines {
                Ok(lines) => {
                    let set: HashSet<String> = lines.into_iter().collect();
                    Ok(set)
                }
                Err(_) => Err(CoreError::InvalidParam(format!(
                    "dict:{:?} can't read file",
                    dict.name
                ))),
            }
        }
    }
}
