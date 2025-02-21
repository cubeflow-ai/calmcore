use std::{collections::HashMap, sync::Arc};

use async_graphql::{Context, Json, Object};
use calmcore::util::{json_to_value_none_schema, CoreError, CoreResult};
use proto::{
    calmserver::{SearchResponse, Status},
    core::{Field, Record, Schema},
};

use crate::service::Service;

use super::models::{result_wrapper, GqlField};

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn create_engine<'a>(
        &self,
        ctx: &Context<'a>,
        engine_name: String,
        #[graphql(default)] fields: Vec<GqlField>,
        metadata: Option<serde_json::Value>,
    ) -> CoreResult<Json<Status>> {
        let core = ctx.data_unchecked::<Arc<Service>>().core();

        let fields = fields
            .into_iter()
            .map(|f| Ok((f.name.clone(), f.try_into()?)))
            .collect::<CoreResult<HashMap<String, Field>>>()?;

        let metadata = if let Some(m) = metadata {
            let value = json_to_value_none_schema(m)?;
            Some(value.to_obj())
        } else {
            None
        };

        let _ = core.create_engine(Schema {
            name: engine_name.clone(),
            schemaless: false,
            fields,
            metadata,
        })?;

        Ok(Json(Status {
            code: 0,
            message: format!("create with schema:{} success", engine_name),
        }))
    }

    // get space by space name
    async fn get_space<'a>(
        &self,
        ctx: &Context<'a>,
        name: String,
    ) -> CoreResult<Json<serde_json::Value>> {
        let engine = ctx.data_unchecked::<Arc<Service>>().get_engine(&name)?;

        let data = serde_json::json!({
            "schema": engine.scope().schema,
            "fields": engine.scope().user_fields.read().unwrap().values().collect::<Vec<&Arc<Field>>>(),
        });

        Ok(Json(data))
    }
    // list all spaces
    async fn list_spaces<'a>(&self, ctx: &Context<'a>) -> CoreResult<Json<Vec<String>>> {
        Ok(Json(
            ctx.data_unchecked::<Arc<Service>>().core().list_engine()?,
        ))
    }

    //search
    pub async fn insert_record<'a>(
        &self,
        ctx: &Context<'a>,
        engine_name: String,
        record_name: String,
        data: serde_json::Value,
        marker: Option<String>,
    ) -> CoreResult<Json<Vec<CoreError>>> {
        let data = serde_json::to_vec(&data)?;

        let resp = ctx
            .data_unchecked::<Arc<Service>>()
            .core()
            .get_engine(&engine_name)?
            .mutate(
                vec![calmcore::Action::Insert(Record {
                    name: record_name,
                    data,
                    ..Default::default()
                })],
                marker,
            )?;

        Ok(Json(resp))
    }

    pub async fn delete_record<'a>(
        &self,
        ctx: &Context<'a>,
        engine_name: String,
        record_name: String,
    ) -> CoreResult<Json<Vec<CoreError>>> {
        let resp = ctx
            .data_unchecked::<Arc<Service>>()
            .core()
            .get_engine(&engine_name)?
            .mutate(
                vec![calmcore::Action::Delete(Record {
                    name: record_name,
                    ..Default::default()
                })],
                None,
            )?;

        Ok(Json(resp))
    }

    pub async fn upsert_record<'a>(
        &self,
        ctx: &Context<'a>,
        engine_name: String,
        record_name: String,
        data: serde_json::Value,
    ) -> CoreResult<Json<Vec<CoreError>>> {
        let data = serde_json::to_vec(&data)?;
        let resp = ctx
            .data_unchecked::<Arc<Service>>()
            .core()
            .get_engine(&engine_name)?
            .mutate(
                vec![calmcore::Action::Upsert(Record {
                    name: record_name,
                    data,
                    ..Default::default()
                })],
                None,
            )?;

        Ok(Json(resp))
    }

    pub async fn search_sql<'a>(
        &self,
        ctx: &Context<'a>,
        engine_name: String,
        sql: String,
    ) -> CoreResult<Json<result_wrapper::SearchResponseWrapper>> {
        let start = std::time::Instant::now();

        let result = ctx
            .data_unchecked::<Arc<Service>>()
            .core()
            .get_engine(&engine_name)?
            .sql(&sql)?;

        let resp = SearchResponse {
            status: None,
            result: Some(result),
            timeuse_mill: start.elapsed().as_millis() as u32,
        };

        Ok(Json(result_wrapper::SearchResponseWrapper::new(resp)))
    }
}
