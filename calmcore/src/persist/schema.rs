use std::{
    fs,
    path::{Path, PathBuf},
};

use proto::core::{Fields, Schema};

use crate::util::CoreResult;

use super::pos_write;

pub struct SchemaStore {
    path: PathBuf,
}

const SCHEMA: &str = "schema.json";
const USER_SCHEMA: &str = "user_schema.json";

impl SchemaStore {
    pub fn new(path: &Path) -> CoreResult<Self> {
        let path = path.join("schema");
        fs::create_dir_all(&path)?;
        Ok(Self { path })
    }

    pub(crate) fn write_user_schema(&self, fs: Vec<proto::core::Field>) -> CoreResult<()> {
        let fs = Fields { fields: fs };

        pos_write(
            self.path.join(USER_SCHEMA),
            &serde_json::to_vec_pretty(&fs)?,
        )
    }

    pub(crate) fn read_user_schema(&self) -> CoreResult<Fields> {
        let data = fs::read_to_string(self.path.join(USER_SCHEMA))?;
        Ok(serde_json::from_str(data.as_str())?)
    }

    pub(crate) fn write_schema(&self, schema: &Schema) -> CoreResult<()> {
        pos_write(self.path.join(SCHEMA), &serde_json::to_vec_pretty(schema)?)
    }

    pub(crate) fn read_schema(&self) -> CoreResult<Schema> {
        let data = fs::read_to_string(self.path.join(SCHEMA))?;
        Ok(serde_json::from_str(data.as_str())?)
    }
}
