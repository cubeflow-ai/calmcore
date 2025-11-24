use crate::engine::Engine;
use datafusion::arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};

/// Prepared statement 信息
pub struct PreparedStatement {
    pub query: String,
}

/// Cursor 状态 - 流式游标
pub struct CursorState {
    /// 查询 SQL
    query: String,
    /// 当前批次索引
    batch_index: usize,
    /// 当前批次内的行索引
    row_in_batch: usize,
    /// 缓存的批次数据 (只缓存当前批次)
    cached_batches: Vec<RecordBatch>,
    /// 是否已完全加载
    exhausted: bool,
}

impl CursorState {
    pub fn new(query: String) -> Self {
        Self {
            query,
            batch_index: 0,
            row_in_batch: 0,
            cached_batches: Vec::new(),
            exhausted: false,
        }
    }

    /// 流式获取数据 - 按需加载批次
    pub async fn fetch_rows(
        &mut self,
        engine: Arc<Engine>,
        count: usize,
    ) -> io::Result<Option<RecordBatch>> {
        if self.exhausted {
            return Ok(None);
        }

        // 如果还没有加载任何数据,执行查询
        if self.cached_batches.is_empty() {
            log::info!("🔄 Executing query for cursor: {}", self.query);
            let result = engine
                .execute_sql(&self.query)
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

            // 先缓存第一个 batch
            self.cached_batches.push(result.batch);
        }

        // 收集需要的行
        let mut collected_rows = Vec::new();
        let mut remaining = count;

        while remaining > 0 && !self.exhausted {
            if self.batch_index >= self.cached_batches.len() {
                // 如果是单批次查询结果,标记为耗尽
                self.exhausted = true;
                break;
            }

            let current_batch = &self.cached_batches[self.batch_index];
            let available_in_batch = current_batch.num_rows() - self.row_in_batch;

            if available_in_batch == 0 {
                // 当前批次已用完,移动到下一个批次
                self.batch_index += 1;
                self.row_in_batch = 0;
                continue;
            }

            // 从当前批次切片
            let take = remaining.min(available_in_batch);
            let sliced = current_batch.slice(self.row_in_batch, take);
            collected_rows.push(sliced);

            self.row_in_batch += take;
            remaining -= take;

            // 如果当前批次用完了,移动到下一个
            if self.row_in_batch >= current_batch.num_rows() {
                self.batch_index += 1;
                self.row_in_batch = 0;

                // 检查是否还有更多批次
                if self.batch_index >= self.cached_batches.len() {
                    self.exhausted = true;
                    break;
                }
            }
        }

        if collected_rows.is_empty() {
            return Ok(None);
        }

        // 合并所有切片
        if collected_rows.len() == 1 {
            Ok(Some(collected_rows.into_iter().next().unwrap()))
        } else {
            // 合并多个批次
            let schema = collected_rows[0].schema();
            let merged = datafusion::arrow::compute::concat_batches(&schema, &collected_rows)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            Ok(Some(merged))
        }
    }
}

/// Cursor 管理器
pub struct CursorManager {
    /// Prepared statements: stmt_id -> PreparedStatement
    prepared_stmts: Arc<Mutex<HashMap<u32, PreparedStatement>>>,
    /// Cursors: cursor_name -> CursorState
    cursors: Arc<Mutex<HashMap<String, CursorState>>>,
    /// Statement ID 计数器
    next_stmt_id: Arc<Mutex<u32>>,
    /// Engine 引用 (公开以供 execute_query 使用)
    pub(super) engine: Arc<Engine>,
}

impl CursorManager {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self {
            prepared_stmts: Arc::new(Mutex::new(HashMap::new())),
            cursors: Arc::new(Mutex::new(HashMap::new())),
            next_stmt_id: Arc::new(Mutex::new(1)),
            engine,
        }
    }

    /// 准备语句
    pub fn prepare_statement(&self, query: &str) -> io::Result<u32> {
        let mut next_id = self.next_stmt_id.lock().unwrap();
        let stmt_id = *next_id;
        *next_id += 1;
        drop(next_id);

        let stmt = PreparedStatement {
            query: query.to_string(),
        };

        self.prepared_stmts.lock().unwrap().insert(stmt_id, stmt);
        log::info!("📝 Prepared statement ID={}: {}", stmt_id, query);

        Ok(stmt_id)
    }

    /// 获取 prepared statement 的查询
    pub fn get_prepared_query(&self, stmt_id: u32) -> Option<String> {
        self.prepared_stmts
            .lock()
            .unwrap()
            .get(&stmt_id)
            .map(|s| s.query.clone())
    }

    /// 关闭 prepared statement
    pub fn close_statement(&self, stmt_id: u32) {
        self.prepared_stmts.lock().unwrap().remove(&stmt_id);
        log::info!("🗑️  Closed prepared statement ID={}", stmt_id);
    }

    /// 声明游标
    pub fn declare_cursor(&self, cursor_name: String, query: String) -> io::Result<()> {
        log::info!("📌 Declaring cursor '{}' for: {}", cursor_name, query);

        let cursor_state = CursorState::new(query);
        self.cursors
            .lock()
            .unwrap()
            .insert(cursor_name.clone(), cursor_state);

        log::info!("✅ Cursor '{}' declared successfully", cursor_name);
        Ok(())
    }

    /// 从游标获取数据
    pub async fn fetch_from_cursor(
        &self,
        cursor_name: &str,
        count: usize,
    ) -> io::Result<Option<RecordBatch>> {
        log::info!("🔍 Fetching {} rows from cursor '{}'", count, cursor_name);

        // 取出 cursor state
        let mut cursors = self.cursors.lock().unwrap();
        let cursor_state = cursors.get_mut(cursor_name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Cursor '{}' not found", cursor_name),
            )
        })?;

        // 临时取出来执行 async 操作
        let query = cursor_state.query.clone();
        let batch_index = cursor_state.batch_index;
        let row_in_batch = cursor_state.row_in_batch;
        let cached_batches = std::mem::take(&mut cursor_state.cached_batches);
        let exhausted = cursor_state.exhausted;

        // 释放锁
        drop(cursors);

        // 重建状态并执行 fetch
        let mut temp_state = CursorState {
            query,
            batch_index,
            row_in_batch,
            cached_batches,
            exhausted,
        };

        let result = temp_state.fetch_rows(self.engine.clone(), count).await;

        // 恢复状态
        let mut cursors = self.cursors.lock().unwrap();
        if let Some(cursor_state) = cursors.get_mut(cursor_name) {
            cursor_state.batch_index = temp_state.batch_index;
            cursor_state.row_in_batch = temp_state.row_in_batch;
            cursor_state.cached_batches = temp_state.cached_batches;
            cursor_state.exhausted = temp_state.exhausted;
        }

        result
    }

    /// 关闭游标
    pub fn close_cursor(&self, cursor_name: &str) -> io::Result<()> {
        let removed = self.cursors.lock().unwrap().remove(cursor_name).is_some();

        if removed {
            log::info!("🗑️  Closed cursor '{}'", cursor_name);
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Cursor '{}' not found", cursor_name),
            ))
        }
    }
}
