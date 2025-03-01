// This file is @generated by prost-build.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Mutate {
    #[prost(enumeration = "mutate::Action", tag = "1")]
    pub action: i32,
    #[prost(message, optional, tag = "2")]
    pub record: ::core::option::Option<super::core::Record>,
}
/// Nested message and enum types in `Mutate`.
pub mod mutate {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum Action {
        Insert = 0,
        Delete = 1,
        Upsert = 2,
    }
    impl Action {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Self::Insert => "Insert",
                Self::Delete => "Delete",
                Self::Upsert => "Upsert",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "Insert" => Some(Self::Insert),
                "Delete" => Some(Self::Delete),
                "Upsert" => Some(Self::Upsert),
                _ => None,
            }
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Status {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub message: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MutateRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub datas: ::prost::alloc::vec::Vec<Mutate>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MutateResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<Status>,
    #[prost(message, repeated, tag = "2")]
    pub record_status: ::prost::alloc::vec::Vec<Status>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(oneof = "search_request::Query", tags = "2, 3")]
    pub query: ::core::option::Option<search_request::Query>,
}
/// Nested message and enum types in `SearchRequest`.
pub mod search_request {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Query {
        #[prost(message, tag = "2")]
        Cql(super::super::core::Query),
        #[prost(string, tag = "3")]
        Sql(::prost::alloc::string::String),
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<Status>,
    #[prost(message, optional, tag = "2")]
    pub result: ::core::option::Option<super::core::QueryResult>,
    #[prost(uint32, tag = "3")]
    pub timeuse_mill: u32,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetRequest {
    #[prost(string, tag = "1")]
    pub engine_name: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub id: u64,
    #[prost(string, tag = "3")]
    pub record_name: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<Status>,
    #[prost(message, optional, tag = "2")]
    pub record: ::core::option::Option<super::core::Record>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LoadEngineRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LoadEngineResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<Status>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateEngineRequest {
    #[prost(message, optional, tag = "1")]
    pub schema: ::core::option::Option<super::core::Schema>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateEngineResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<Status>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReleaseEngineRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReleaseEngineResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<Status>,
}
/// Generated client implementations.
pub mod server_client {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
        clippy::let_unit_value,
    )]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct ServerClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ServerClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ServerClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + std::marker::Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + std::marker::Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> ServerClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + std::marker::Send + std::marker::Sync,
        {
            ServerClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        pub async fn get(
            &mut self,
            request: impl tonic::IntoRequest<super::GetRequest>,
        ) -> std::result::Result<tonic::Response<super::GetResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/calmserver.Server/Get");
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("calmserver.Server", "Get"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn mutate(
            &mut self,
            request: impl tonic::IntoRequest<super::MutateRequest>,
        ) -> std::result::Result<tonic::Response<super::MutateResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/calmserver.Server/Mutate");
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("calmserver.Server", "Mutate"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn search(
            &mut self,
            request: impl tonic::IntoRequest<super::SearchRequest>,
        ) -> std::result::Result<tonic::Response<super::SearchResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/calmserver.Server/Search");
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("calmserver.Server", "Search"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn create_engine(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateEngineRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateEngineResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/calmserver.Server/CreateEngine",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("calmserver.Server", "CreateEngine"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn load_engine(
            &mut self,
            request: impl tonic::IntoRequest<super::LoadEngineRequest>,
        ) -> std::result::Result<
            tonic::Response<super::LoadEngineResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/calmserver.Server/LoadEngine",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("calmserver.Server", "LoadEngine"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn release_engine(
            &mut self,
            request: impl tonic::IntoRequest<super::ReleaseEngineRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ReleaseEngineResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/calmserver.Server/ReleaseEngine",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("calmserver.Server", "ReleaseEngine"));
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod server_server {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
        clippy::let_unit_value,
    )]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with ServerServer.
    #[async_trait]
    pub trait Server: std::marker::Send + std::marker::Sync + 'static {
        async fn get(
            &self,
            request: tonic::Request<super::GetRequest>,
        ) -> std::result::Result<tonic::Response<super::GetResponse>, tonic::Status>;
        async fn mutate(
            &self,
            request: tonic::Request<super::MutateRequest>,
        ) -> std::result::Result<tonic::Response<super::MutateResponse>, tonic::Status>;
        async fn search(
            &self,
            request: tonic::Request<super::SearchRequest>,
        ) -> std::result::Result<tonic::Response<super::SearchResponse>, tonic::Status>;
        async fn create_engine(
            &self,
            request: tonic::Request<super::CreateEngineRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateEngineResponse>,
            tonic::Status,
        >;
        async fn load_engine(
            &self,
            request: tonic::Request<super::LoadEngineRequest>,
        ) -> std::result::Result<
            tonic::Response<super::LoadEngineResponse>,
            tonic::Status,
        >;
        async fn release_engine(
            &self,
            request: tonic::Request<super::ReleaseEngineRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ReleaseEngineResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct ServerServer<T> {
        inner: Arc<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    impl<T> ServerServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for ServerServer<T>
    where
        T: Server,
        B: Body + std::marker::Send + 'static,
        B::Error: Into<StdError> + std::marker::Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            match req.uri().path() {
                "/calmserver.Server/Get" => {
                    #[allow(non_camel_case_types)]
                    struct GetSvc<T: Server>(pub Arc<T>);
                    impl<T: Server> tonic::server::UnaryService<super::GetRequest>
                    for GetSvc<T> {
                        type Response = super::GetResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Server>::get(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = GetSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/calmserver.Server/Mutate" => {
                    #[allow(non_camel_case_types)]
                    struct MutateSvc<T: Server>(pub Arc<T>);
                    impl<T: Server> tonic::server::UnaryService<super::MutateRequest>
                    for MutateSvc<T> {
                        type Response = super::MutateResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::MutateRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Server>::mutate(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = MutateSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/calmserver.Server/Search" => {
                    #[allow(non_camel_case_types)]
                    struct SearchSvc<T: Server>(pub Arc<T>);
                    impl<T: Server> tonic::server::UnaryService<super::SearchRequest>
                    for SearchSvc<T> {
                        type Response = super::SearchResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SearchRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Server>::search(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = SearchSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/calmserver.Server/CreateEngine" => {
                    #[allow(non_camel_case_types)]
                    struct CreateEngineSvc<T: Server>(pub Arc<T>);
                    impl<
                        T: Server,
                    > tonic::server::UnaryService<super::CreateEngineRequest>
                    for CreateEngineSvc<T> {
                        type Response = super::CreateEngineResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateEngineRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Server>::create_engine(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = CreateEngineSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/calmserver.Server/LoadEngine" => {
                    #[allow(non_camel_case_types)]
                    struct LoadEngineSvc<T: Server>(pub Arc<T>);
                    impl<T: Server> tonic::server::UnaryService<super::LoadEngineRequest>
                    for LoadEngineSvc<T> {
                        type Response = super::LoadEngineResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::LoadEngineRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Server>::load_engine(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = LoadEngineSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/calmserver.Server/ReleaseEngine" => {
                    #[allow(non_camel_case_types)]
                    struct ReleaseEngineSvc<T: Server>(pub Arc<T>);
                    impl<
                        T: Server,
                    > tonic::server::UnaryService<super::ReleaseEngineRequest>
                    for ReleaseEngineSvc<T> {
                        type Response = super::ReleaseEngineResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ReleaseEngineRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Server>::release_engine(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = ReleaseEngineSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        let mut response = http::Response::new(empty_body());
                        let headers = response.headers_mut();
                        headers
                            .insert(
                                tonic::Status::GRPC_STATUS,
                                (tonic::Code::Unimplemented as i32).into(),
                            );
                        headers
                            .insert(
                                http::header::CONTENT_TYPE,
                                tonic::metadata::GRPC_CONTENT_TYPE,
                            );
                        Ok(response)
                    })
                }
            }
        }
    }
    impl<T> Clone for ServerServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    /// Generated gRPC service name
    pub const SERVICE_NAME: &str = "calmserver.Server";
    impl<T> tonic::server::NamedService for ServerServer<T> {
        const NAME: &'static str = SERVICE_NAME;
    }
}
