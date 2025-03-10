use std::{
    fs::{self, File},
    io::Cursor,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow::array::{RecordBatch, UInt32Array};
use byteorder::{BigEndian, ReadBytesExt};
use bytes::Bytes;
use memmap2::Mmap;
use parquet::{
    arrow::{
        arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder},
        ProjectionMask,
    },
    file::reader::SerializedFileReader,
};

use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::util::{group_range, CoreError, CoreResult};

/// 表示一个存储在磁盘上的数据块
pub struct Block {
    start: u64,
    end: u64,
    block_start: usize,
    block_end: usize,
}

impl Block {
    /// 从文件名解析一个块
    fn new(start: u64, end: u64, block_start: usize, block_end: usize) -> Self {
        Self {
            start,
            end,
            block_start,
            block_end,
        }
    }

    fn read(
        &self,
        data: &Mmap,
        metadata: ArrowReaderMetadata,
        projection: Option<&[String]>,
        ids: Vec<u32>,
    ) -> CoreResult<Option<RecordBatch>> {
        let data = unsafe {
            std::mem::transmute::<&[u8], &'static [u8]>(&data[self.block_start..self.block_end])
        };

        let mask = match projection {
            Some(projection) => ProjectionMask::columns(
                metadata.parquet_schema(),
                projection.iter().map(String::as_str),
            ),
            None => ProjectionMask::all(),
        };

        let builder =
            ParquetRecordBatchReaderBuilder::new_with_metadata(Bytes::from_owner(data), metadata);

        let mut projected_reader = builder
            .with_projection(mask)
            .with_batch_size((self.end - self.start) as usize)
            .build()
            .unwrap();

        let batch = projected_reader.next();
        match batch {
            Some(batch) => {
                let indices = UInt32Array::from(ids);
                Ok(Some(arrow::compute::take_record_batch(&batch?, &indices)?))
            }
            None => Ok(None),
        }
    }

    fn read_scheme(&self, data: &Mmap) -> CoreResult<ArrowReaderMetadata> {
        Ok(ArrowReaderMetadata::load(
            &Bytes::copy_from_slice(&data[self.block_start..self.block_end]),
            Default::default(),
        )?)
    }
}

/// 管理多个数据块的读取器
pub struct BlockReader {
    blocks: Vec<Block>,
    metadata: ArrowReaderMetadata,
    data: Mmap,
}

impl BlockReader {
    /// 从目录创建一个新的BlockReader
    pub fn new(dir: &Path) -> CoreResult<Self> {
        let mut index = File::open(dir.join("index"))?;

        let mut blocks = Vec::new();
        loop {
            let start = match index.read_u64::<BigEndian>() {
                Ok(s) => s,
                Err(e) => {
                    //if e is eof then break
                    todo!("{:?}", e);
                }
            };
            let end = index.read_u64::<BigEndian>()?;
            let block_start = index.read_u64::<BigEndian>()? as usize;
            let block_end = index.read_u64::<BigEndian>()? as usize;
            blocks.push(Block {
                start,
                end,
                block_start,
                block_end,
            });

            break;
        }

        let mut data = unsafe { Mmap::map(&File::open(dir.join("data"))?)? };

        let metadata = blocks.get(0).unwrap().read_scheme(&data)?;

        Ok(Self {
            blocks,
            metadata,
            data,
        })
    }

    /// 读取给定ID列表的数据，返回一个包含请求记录的RecordBatch
    pub fn read(
        &self,
        projection: Option<&[String]>,
        ids: &[u64],
    ) -> CoreResult<Option<RecordBatch>> {
        if ids.is_empty() {
            return Ok(None);
        }

        let blocks = group_range(&self.blocks, ids, |b1, _, k| {
            let k = *k;

            if k < b1.start {
                std::cmp::Ordering::Less
            } else if k <= b1.end {
                std::cmp::Ordering::Equal
            } else {
                std::cmp::Ordering::Greater
            }
        });

        if blocks.is_empty() {
            return Ok(None);
        }

        let batches = blocks
            .par_iter()
            .filter_map(|(b, ids)| {
                b.read(
                    &self.data,
                    self.metadata.clone(),
                    projection,
                    ids.iter().map(|&id| (id - b.start) as u32).collect(),
                )
                .unwrap_or_else(|e| panic!("block search err:{:?}", e))
            })
            .collect::<Vec<_>>();

        if batches.is_empty() {
            return Ok(None);
        }
        let schema = batches[0].schema();
        Ok(Some(arrow::compute::concat_batches(&schema, &batches)?))
    }
}
