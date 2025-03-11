use std::{
    fs::{self, File},
    io::Cursor,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow::array::{RecordBatch, UInt32Array};
use byteorder::{BigEndian, ReadBytesExt};
use bytes::Bytes;
use log::error;
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

#[derive(Debug)]
pub struct Block {
    start: u64,
    end: u64,
    block_start: usize,
    block_end: usize,
    schema: ArrowReaderMetadata,
}

impl Block {
    /// 从文件名解析一个块
    fn new(
        data: &Mmap,
        start: u64,
        end: u64,
        block_start: usize,
        block_end: usize,
    ) -> CoreResult<Self> {
        Ok(Self {
            start,
            end,
            block_start,
            block_end,
            schema: Self::read_scheme(data, block_start, block_end)?,
        })
    }

    fn read(
        &self,
        data: &Mmap,
        projection: Option<&[String]>,
        ids: Vec<u32>,
    ) -> CoreResult<Option<RecordBatch>> {
        let data = unsafe {
            std::mem::transmute::<&[u8], &'static [u8]>(&data[self.block_start..self.block_end])
        };

        let mask = match projection {
            Some(projection) => ProjectionMask::columns(
                self.schema.parquet_schema(),
                projection.iter().map(String::as_str),
            ),
            None => ProjectionMask::all(),
        };

        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
            Bytes::from_owner(data),
            self.schema.clone(),
        );

        let mut projected_reader = builder
            .with_projection(mask)
            .with_batch_size((self.end - self.start + 1) as usize)
            .build()
            .unwrap();

        let batch = projected_reader.next();

        match batch {
            Some(Ok(batch)) => {
                let indices = UInt32Array::from(ids);
                Ok(Some(arrow::compute::take_record_batch(&batch, &indices)?))
            }
            Some(Err(e)) => {
                error!("block read err:{:?}", e);
                Err(CoreError::from(e))
            }
            None => Ok(None),
        }
    }

    fn read_scheme(data: &Mmap, start: usize, end: usize) -> CoreResult<ArrowReaderMetadata> {
        Ok(ArrowReaderMetadata::load(
            &Bytes::copy_from_slice(&data[start..end]),
            Default::default(),
        )?)
    }
}

/// 管理多个数据块的读取器
pub struct BlockReader {
    blocks: Vec<Block>,
    data: Mmap,
}

impl BlockReader {
    /// 从目录创建一个新的BlockReader
    pub fn new(dir: &Path) -> CoreResult<Self> {
        let mut index = File::open(dir.join("index"))?;

        let data = unsafe { Mmap::map(&File::open(dir.join("data"))?)? };

        let mut blocks = Vec::new();
        loop {
            let start = match index.read_u64::<BigEndian>() {
                Ok(s) => s,
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        return Err(CoreError::from(e));
                    }
                }
            };
            let end = index.read_u64::<BigEndian>()?;
            let block_start = index.read_u64::<BigEndian>()? as usize;
            let block_end = index.read_u64::<BigEndian>()? as usize;
            blocks.push(Block::new(&data, start, end, block_start, block_end)?);
        }

        blocks.sort_by_key(|b| b.start);

        Ok(Self { blocks, data })
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

mod tests {
    use crate::index_store::segment_mem::MemSegment;

    use super::*;

    #[test]
    fn test_block_reader() {
        let path = PathBuf::from(
            "/Users/sunjian/rustworkspace/calmcore/big_data/big_test/segments/1-3955000/_source",
        );
        let reader = BlockReader::new(Path::new(
            "/Users/sunjian/rustworkspace/calmcore/big_data/big_test/segments/1-3955000/_source",
        ))
        .unwrap();

        let data = reader.read(None, &[1025]).unwrap();
        println!("{:?}", data);

        // let data = unsafe { Mmap::map(&File::open(path.join("data")).unwrap()).unwrap() };

        // let data = data[9547..19149].to_vec();

        // let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from_owner(data)).unwrap();

        // let batch = builder
        //     .with_projection(ProjectionMask::all())
        //     .with_batch_size((1024) as usize)
        //     .build()
        //     .unwrap()
        //     .next()
        //     .unwrap()
        //     .unwrap();

        // let indices = UInt32Array::from(vec![0]);

        // println!(
        //     "{:?}",
        //     arrow::compute::take_record_batch(&batch, &indices).unwrap()
        // );
    }
}
