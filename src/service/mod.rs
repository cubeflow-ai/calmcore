use std::sync::Arc;

use calmcore::{util::CoreResult, CalmCore, Engine};

pub struct Service {
    calm: CalmCore,
    #[allow(dead_code)] //TODO FIXME
    manager_addr: String,
}

impl Service {
    pub fn new(manager_addr: String, data_path: String) -> CoreResult<Self> {
        let calm = CalmCore::new(&data_path)?;
        Ok(Self { calm, manager_addr })
    }

    pub fn core(&self) -> &CalmCore {
        &self.calm
    }

    pub fn get_engine(&self, name: &str) -> CoreResult<Arc<Engine>> {
        self.calm.get_engine(name)
    }
}
