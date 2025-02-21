use std::{borrow::Cow, error::Error, path::PathBuf, str::FromStr};

use croaring::{Bitmap, Portable};
use mem_btree::persist;

use crate::util::{CoreError, CoreResult};

struct BitmapDeserializer;

impl persist::KVDeserializer<Vec<u8>, Bitmap> for BitmapDeserializer {
    fn deserialize_value(&self, v: &[u8]) -> std::result::Result<Bitmap, Box<dyn Error>> {
        Bitmap::try_deserialize::<Portable>(v).ok_or_else(|| {
            CoreError::DecodeError("decode bitmap err".to_string(), v.to_vec()).into()
        })
    }

    fn serialize_key<'a>(&self, k: &'a Vec<u8>) -> Cow<'a, [u8]> {
        Cow::Borrowed(k.as_slice())
    }
}

pub struct DiskInvertIndex {
    term_record_index: persist::TreeReader<Vec<u8>, Bitmap>,
}

impl DiskInvertIndex {
    pub fn new(path: PathBuf) -> CoreResult<Self> {
        let term_record_index = persist::TreeReader::new(&path, Box::new(BitmapDeserializer {}))?;
        Ok(Self { term_record_index })
    }

    pub(crate) fn get(&self, key: &Vec<u8>) -> Option<Bitmap> {
        self.term_record_index.get(&key)
    }

    pub(crate) fn pre_range<F>(&self, start: Option<&Vec<u8>>, mut f: F)
    where
        F: FnMut(&[u8], &Bitmap) -> bool,
    {
        let mut iter = self.term_record_index.iter();
        if let Some(start) = start {
            iter.seek(start);
        }
        while let Some(item) = iter.next() {
            if !f(&item.0, &item.1) {
                break;
            }
        }
    }
}
