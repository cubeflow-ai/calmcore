use croaring::{Bitmap, Portable};
use proto::core::Hit;
use rkyv::{
    api::high::to_bytes_with_alloc, ser::allocator::Arena, util::AlignedVec, Archive, Deserialize,
    Serialize,
};

use crate::util::{CoreError, CoreResult};

/// Extract a column from RecordBatch and convert it to a specific Arrow array type by index
///
/// # Parameters
/// * `batch` - The RecordBatch to extract data from
/// * `col_index` - The column index
/// * `array_type` - The target Arrow array type
///
/// # Example
/// ```
/// let ids: CoreResult<UInt64Array> = data_column_index!(batch, 0, UInt64Array);
/// ```
#[macro_export]
macro_rules! data_column_index {
    ($batch:expr, $col_index:expr, $array_type:ty) => {{
        let column = $batch.column($col_index);
        match column.as_any().downcast_ref::<$array_type>() {
            Some(array) => Ok(array.clone()),
            None => Err(CoreError::Type(format!(
                "Failed to downcast column {} to {}",
                $col_index,
                stringify!($array_type)
            ))),
        }
    }};
}

/// Extract a column from RecordBatch and convert it to a specific Arrow array type by name
///
/// # Parameters
/// * `batch` - The RecordBatch to extract data from
/// * `col_name` - The column name
/// * `array_type` - The target Arrow array type
///
/// # Example
/// ```
/// let ids: CoreResult<UInt64Array> = data_column_name!(batch, "id", UInt64Array);
/// ```
#[macro_export]
macro_rules! data_column_name {
    ($batch:expr, $col_name:expr, $array_type:ty) => {{
        let schema = $batch.schema();
        let col_index = match schema.index_of($col_name) {
            Some(index) => index,
            None => {
                return Err(CoreError::Type(format!(
                    "Column {} not found in schema",
                    $col_name
                )))
            }
        };
        let column = $batch.column(col_index);
        match column.as_any().downcast_ref::<$array_type>() {
            Some(array) => Ok(array.clone()),
            None => Err(CoreError::Type(format!(
                "Failed to downcast column {} to {}",
                $col_name,
                stringify!($array_type)
            ))),
        }
    }};
}

pub struct TermPositionWriter {
    pub name: String,
    pub ids: Vec<u32>,
    pub index: Vec<u32>,
    pub length: Vec<u16>,
    pub offsets: Vec<u32>,
}

impl TermPositionWriter {
    pub fn release(&self) -> TermPosition {
        let mut bitmap = Bitmap::from_iter(self.ids.iter().cloned());

        bitmap.run_optimize();

        let ids = bitmap.serialize::<Portable>();

        TermPosition {
            name: self.name.clone(),
            ids,
            index: self.index.clone(),
            length: self.length.clone(),
            offsets: self.offsets.clone(),
        }
    }
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Default)]
pub struct TermPosition {
    pub name: String,
    pub ids: Vec<u8>,
    pub index: Vec<u32>,
    pub length: Vec<u16>,
    pub offsets: Vec<u32>,
}

impl TermPosition {
    pub fn serializer(&self, arena: &mut Arena) -> CoreResult<AlignedVec> {
        to_bytes_with_alloc::<_, rkyv::rancor::Error>(self, arena.acquire()).map_err(|e| {
            CoreError::EcodeError(format!(
                "Failed to serialize TermPosition: {:?} err:{:?}",
                self, e
            ))
        })
    }

    pub fn deserializer(bytes: &[u8]) -> CoreResult<&ArchivedTermPosition> {
        rkyv::access::<ArchivedTermPosition, rkyv::rancor::Error>(&bytes[..]).map_err(|e| {
            CoreError::DecodeError(format!("Failed to deserialize err:{:?}", e), bytes.to_vec())
        })
    }
}

pub enum IdList {
    BitMap(Bitmap),
    Hits(Vec<Hit>),
}
