use crate::CoreResult;
use roaring::RoaringBitmap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::Path;

/// Union bitmap 计算和写入工具
pub struct UnionBitmapWriter {
    data_file: BufWriter<File>,
    current_offset: i64,
}

impl UnionBitmapWriter {
    /// 打开现有的 DATA 文件以追加 union bitmaps
    pub fn new(data_path: &Path) -> CoreResult<Self> {
        let mut data_file = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .append(true)
                .open(data_path)
                .map_err(|e| crate::CoreError::IOError(e.to_string()))?,
        );

        // 获取当前文件大小作为起始 offset
        let current_offset = data_file
            .seek(SeekFrom::End(0))
            .map_err(|e| crate::CoreError::IOError(e.to_string()))?
            as i64;

        Ok(Self {
            data_file,
            current_offset,
        })
    }

    /// 写入一个 union bitmap 并返回其 offset
    pub fn write_union_bitmap(&mut self, bitmap: &RoaringBitmap) -> CoreResult<i64> {
        let offset = self.current_offset;

        // 序列化 bitmap
        let mut bitmap_bytes = Vec::new();
        bitmap.serialize_into(&mut bitmap_bytes).map_err(|e| {
            crate::CoreError::Internal(format!("Failed to serialize bitmap: {}", e))
        })?;

        // 写入大小和数据
        let size = bitmap_bytes.len() as u32;
        self.data_file
            .write_all(&size.to_be_bytes())
            .map_err(|e| crate::CoreError::IOError(e.to_string()))?;
        self.data_file
            .write_all(&bitmap_bytes)
            .map_err(|e| crate::CoreError::IOError(e.to_string()))?;

        // 更新 offset
        self.current_offset += 4 + bitmap_bytes.len() as i64;

        Ok(offset)
    }

    /// 刷新缓冲区
    pub fn flush(&mut self) -> CoreResult<()> {
        self.data_file
            .flush()
            .map_err(|e| crate::CoreError::IOError(e.to_string()))
    }
}

/// 计算子树的 union bitmap
pub fn compute_union_bitmap_for_subtree(
    reader: &mem_btree::persist::TreeReader<impl Clone + PartialOrd, RoaringBitmap>,
    node_offset: i64,
) -> CoreResult<RoaringBitmap> {
    // TODO: 实现递归计算逻辑
    // 这需要访问 TreeReader 的内部节点结构
    // 暂时返回空 bitmap
    Ok(RoaringBitmap::new())
}
