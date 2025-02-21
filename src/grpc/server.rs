#![allow(clippy::needless_update)]
use std::sync::Arc;

use calmcore::{util::CoreError, Action};
use itertools::Itertools;
use proto::calmserver::*;

use crate::service::Service;
pub(crate) struct GrpcServer {
    service: Arc<Service>,
}

impl GrpcServer {
    pub fn new(service: Arc<Service>) -> Self {
        Self { service }
    }
}

macro_rules! get_engine {
    ($self:expr, $native:expr, $name:ident) => {{
        let engine = $self.service.get_engine($native);
        match engine {
            Ok(engine) => engine,
            Err(e) => {
                return Ok(tonic::Response::new($name {
                    status: status(&e),
                    ..Default::default()
                }));
            }
        }
    }};
}

macro_rules! result {
    ($result:expr, $resp:ident) => {{
        match $result {
            Ok(r) => r,
            Err(e) => {
                log::error!("server grpc has error: {:?}", e);
                return Ok(tonic::Response::new($resp {
                    status: status(&e),
                    ..Default::default()
                }));
            }
        }
    }};
}

#[tonic::async_trait]
impl proto::calmserver::server_server::Server for GrpcServer {
    async fn get(
        &self,
        request: tonic::Request<GetRequest>,
    ) -> Result<tonic::Response<GetResponse>, tonic::Status> {
        let req = request.into_inner();
        let engine = get_engine!(self, &req.engine_name, GetResponse);

        let record = engine.get(&req.record_name);

        return Ok(tonic::Response::new(GetResponse {
            status: status(&CoreError::Ok),
            record,
        }));
    }

    async fn mutate(
        &self,
        request: tonic::Request<MutateRequest>,
    ) -> Result<tonic::Response<MutateResponse>, tonic::Status> {
        let req = request.into_inner();

        let engine = get_engine!(self, &req.name, MutateResponse);
        let actions = req
            .datas
            .into_iter()
            .filter(|a| a.record.is_some())
            .map(|a| match a.action() {
                mutate::Action::Insert => Action::Insert(a.record.unwrap()),
                mutate::Action::Delete => Action::Delete(a.record.unwrap()),
                mutate::Action::Upsert => Action::Upsert(a.record.unwrap()),
            })
            .collect_vec();

        let results: Result<Vec<CoreError>, CoreError> = engine.mutate(actions, None);
        let results = result!(results, MutateResponse)
            .into_iter()
            .map(|e| {
                if e.is_ok() {
                    Status::default()
                } else {
                    status(&e).unwrap()
                }
            })
            .collect_vec();

        Ok(tonic::Response::new(MutateResponse {
            status: status(&CoreError::Ok),
            record_status: results,
        }))
    }

    async fn search(
        &self,
        request: tonic::Request<SearchRequest>,
    ) -> Result<tonic::Response<SearchResponse>, tonic::Status> {
        let req = request.into_inner();
        let engine = get_engine!(self, &req.name, SearchResponse);

        let result = req
            .query
            .ok_or_else(|| CoreError::InvalidParam("query is required".to_string()));

        let query = result!(result, SearchResponse);

        let result = match query {
            search_request::Query::Cql(cql) => engine.search(cql),
            search_request::Query::Sql(sql) => engine.sql(&sql),
        };

        let result = result!(result, SearchResponse);

        Ok(tonic::Response::new(SearchResponse {
            status: status(&CoreError::Ok),
            result: Some(result),
            timeuse_mill: 0,
        }))
    }

    async fn create_engine(
        &self,
        request: tonic::Request<CreateEngineRequest>,
    ) -> Result<tonic::Response<CreateEngineResponse>, tonic::Status> {
        let req = request.into_inner();
        log::info!("load engine request: {:?}", req);

        let schema = req
            .schema
            .ok_or_else(|| CoreError::InvalidParam("schema not set".to_string()));
        let schema = result!(schema, CreateEngineResponse);

        let result = self.service.core().create_engine(schema);

        result!(result, CreateEngineResponse);

        Ok(tonic::Response::new(CreateEngineResponse {
            status: status(&CoreError::Ok),
        }))
    }

    async fn load_engine(
        &self,
        request: tonic::Request<LoadEngineRequest>,
    ) -> Result<tonic::Response<LoadEngineResponse>, tonic::Status> {
        let req = request.into_inner();
        log::info!("load engine request: {:?}", req);
        let result = self.service.core().load_engine(&req.name);

        result!(result, LoadEngineResponse);

        Ok(tonic::Response::new(LoadEngineResponse {
            status: status(&CoreError::Ok),
        }))
    }

    async fn release_engine(
        &self,
        request: tonic::Request<ReleaseEngineRequest>,
    ) -> Result<tonic::Response<ReleaseEngineResponse>, tonic::Status> {
        let req = request.into_inner();
        let result = self.service.core().release_engine(&req.name);
        result!(result, ReleaseEngineResponse);
        Ok(tonic::Response::new(ReleaseEngineResponse {
            status: status(&CoreError::Ok),
        }))
    }
}

fn status(e: &CoreError) -> Option<Status> {
    Some(Status {
        code: e.code(),
        message: e.to_string(),
    })
}
