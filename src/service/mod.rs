use crate::service::{ddl::DDLService, dml::DMLService};

pub(crate) mod ddl;
pub(crate) mod dml;

pub struct CalmSerivce {
    ddl: DDLService,
    dml: DMLService,
}
