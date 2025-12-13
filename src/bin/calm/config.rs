use calm::cluster::ClusterConfig;
use calm::engine::EngineConfig;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// 服务器配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// 监听地址
    #[serde(default = "default_host")]
    pub host: String,

    /// GraphQL 服务端口
    #[serde(default = "default_graphql_port")]
    pub graphql_port: Option<u16>,

    /// Elasticsearch 服务端口
    #[serde(default = "default_es_port")]
    pub es_port: Option<u16>,

    /// MySQL 服务端口
    #[serde(default = "default_mysql_port")]
    pub mysql_port: Option<u16>,

    /// 用户名（适用于 MySQL, Elasticsearch 等）
    #[serde(default = "default_user")]
    pub user: String,

    /// 密码（适用于 MySQL, Elasticsearch 等）
    #[serde(default = "default_password")]
    pub password: String,

    /// 日志配置
    #[serde(default)]
    pub log: LogSettings,

    /// 引擎配置
    #[serde(default)]
    pub engine: EngineSettings,

    /// 集群配置（None 表示单机模式，包含分布式查询配置）
    #[serde(default)]
    pub cluster: Option<ClusterSettings>,
}

/// 日志配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogSettings {
    /// 日志级别: trace, debug, info, warn, error
    #[serde(default = "default_log_level")]
    pub level: String,

    /// 日志输出目标: console, file, both
    #[serde(default = "default_log_target")]
    pub target: String,

    /// 日志文件路径（当 target 为 file 或 both 时生效）
    #[serde(default = "default_log_file")]
    pub file: Option<PathBuf>,

    /// 是否启用日志文件轮转
    #[serde(default = "default_log_rotation")]
    pub rotation: bool,

    /// 日志文件最大大小（MB，启用轮转时生效）
    #[serde(default = "default_log_max_size")]
    pub max_size_mb: u64,

    /// 保留的日志文件数量（启用轮转时生效）
    #[serde(default = "default_log_max_files")]
    pub max_files: usize,
}

/// 引擎配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineSettings {
    /// 数据目录
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,

    /// 持久化检查间隔（秒）
    #[serde(default = "default_persist_interval")]
    pub persist_check_interval_secs: u64,

    /// 最大并发持久化的 Partition 数量
    #[serde(default = "default_max_concurrent_persists")]
    pub max_concurrent_persists: usize,

    /// 是否在 flush 后立即检查持久化
    #[serde(default = "default_check_after_flush")]
    pub check_after_flush: bool,
}

// 默认值函数
fn default_host() -> String {
    "127.0.0.1".to_string()
}

fn default_graphql_port() -> Option<u16> {
    Some(9567)
}

fn default_es_port() -> Option<u16> {
    Some(9200)
}

fn default_mysql_port() -> Option<u16> {
    Some(3307)
}

fn default_user() -> String {
    "root".to_string()
}

fn default_password() -> String {
    "calm".to_string()
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("./data")
}

fn default_persist_interval() -> u64 {
    60
}

fn default_max_concurrent_persists() -> usize {
    4
}

fn default_check_after_flush() -> bool {
    true
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_target() -> String {
    "console".to_string()
}

fn default_log_file() -> Option<PathBuf> {
    Some(PathBuf::from("./logs/calm.log"))
}

fn default_log_rotation() -> bool {
    true
}

fn default_log_max_size() -> u64 {
    100
}

fn default_log_max_files() -> usize {
    10
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: default_host(),
            graphql_port: default_graphql_port(),
            es_port: default_es_port(),
            mysql_port: default_mysql_port(),
            user: default_user(),
            password: default_password(),
            log: LogSettings::default(),
            engine: EngineSettings::default(),
            cluster: None,
        }
    }
}

impl Default for LogSettings {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            target: default_log_target(),
            file: default_log_file(),
            rotation: default_log_rotation(),
            max_size_mb: default_log_max_size(),
            max_files: default_log_max_files(),
        }
    }
}

impl Default for EngineSettings {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            persist_check_interval_secs: default_persist_interval(),
            max_concurrent_persists: default_max_concurrent_persists(),
            check_after_flush: default_check_after_flush(),
        }
    }
}

/// 集群配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSettings {
    /// 集群 ID（同一集群的节点必须相同）
    #[serde(default = "default_cluster_id")]
    pub cluster_id: String,

    /// Gossip 监听端口 (UDP)
    #[serde(default = "default_gossip_port")]
    pub gossip_port: u16,

    /// 内部 RPC 服务端口 (TCP)
    #[serde(default = "default_internal_port")]
    pub internal_port: u16,

    /// 种子节点列表（用于初始加入集群）
    #[serde(default)]
    pub seed_nodes: Vec<String>,

    /// Gossip 间隔（毫秒）
    #[serde(default = "default_gossip_interval_ms")]
    pub gossip_interval_ms: u64,

    /// 故障检测超时（秒）
    #[serde(default = "default_failure_timeout_secs")]
    pub failure_timeout_secs: u64,

    /// 可疑状态超时（秒）
    #[serde(default = "default_suspect_timeout_secs")]
    pub suspect_timeout_secs: u64,

    /// 分布式查询配置
    #[serde(default)]
    pub distributed: DistributedSettings,
}

fn default_cluster_id() -> String {
    "".to_string()
}

fn default_gossip_port() -> u16 {
    7946
}

fn default_internal_port() -> u16 {
    7947
}

fn default_gossip_interval_ms() -> u64 {
    500
}

fn default_failure_timeout_secs() -> u64 {
    10
}

fn default_suspect_timeout_secs() -> u64 {
    10
}

fn default_vote_timeout_secs() -> u64 {
    10
}

fn default_min_cluster_size() -> usize {
    3
}

impl Default for ClusterSettings {
    fn default() -> Self {
        Self {
            cluster_id: default_cluster_id(),
            gossip_port: default_gossip_port(),
            internal_port: default_internal_port(),
            seed_nodes: vec![],
            gossip_interval_ms: default_gossip_interval_ms(),
            failure_timeout_secs: default_failure_timeout_secs(),
            suspect_timeout_secs: default_suspect_timeout_secs(),
            distributed: DistributedSettings::default(),
        }
    }
}

/// 分布式查询配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedSettings {
    /// 查询超时时间（毫秒）
    #[serde(default = "default_query_timeout_ms")]
    pub query_timeout_ms: u64,

    /// Shuffle 缓冲区大小（行数）
    #[serde(default = "default_shuffle_buffer_size")]
    pub shuffle_buffer_size: usize,

    /// 最大并发查询数
    #[serde(default = "default_max_concurrent_queries")]
    pub max_concurrent_queries: usize,

    /// 节点间通信端口（RPC）
    #[serde(default = "default_rpc_port")]
    pub rpc_port: u16,

    /// 连接超时时间（毫秒）
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,

    /// 请求超时时间（毫秒）
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
}

fn default_query_timeout_ms() -> u64 {
    30_000
}

fn default_shuffle_buffer_size() -> usize {
    10_000
}

fn default_max_concurrent_queries() -> usize {
    100
}

fn default_rpc_port() -> u16 {
    7947
}

fn default_connect_timeout_ms() -> u64 {
    5_000
}

fn default_request_timeout_ms() -> u64 {
    30_000
}

impl Default for DistributedSettings {
    fn default() -> Self {
        Self {
            query_timeout_ms: default_query_timeout_ms(),
            shuffle_buffer_size: default_shuffle_buffer_size(),
            max_concurrent_queries: default_max_concurrent_queries(),
            rpc_port: default_rpc_port(),
            connect_timeout_ms: default_connect_timeout_ms(),
            request_timeout_ms: default_request_timeout_ms(),
        }
    }
}

impl Config {
    /// 从命令行参数加载配置
    pub fn from_args() -> Result<Self, Box<dyn std::error::Error>> {
        let mut config = Self::default();

        // 解析命令行参数
        let args: Vec<String> = std::env::args().collect();
        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--host" | "-h" => {
                    if i + 1 < args.len() {
                        config.host = args[i + 1].clone();
                        i += 2;
                    } else {
                        return Err("Missing value for --host".into());
                    }
                }
                "--graphql-port" => {
                    if i + 1 < args.len() {
                        config.graphql_port = Some(args[i + 1].parse()?);
                        i += 2;
                    } else {
                        return Err("Missing value for --graphql-port".into());
                    }
                }
                "--es-port" => {
                    if i + 1 < args.len() {
                        config.es_port = Some(args[i + 1].parse()?);
                        i += 2;
                    } else {
                        return Err("Missing value for --es-port".into());
                    }
                }
                "--mysql-port" => {
                    if i + 1 < args.len() {
                        config.mysql_port = Some(args[i + 1].parse()?);
                        i += 2;
                    } else {
                        return Err("Missing value for --mysql-port".into());
                    }
                }
                "--user" | "-u" => {
                    if i + 1 < args.len() {
                        config.user = args[i + 1].clone();
                        i += 2;
                    } else {
                        return Err("Missing value for --user".into());
                    }
                }
                "--password" | "-p" => {
                    if i + 1 < args.len() {
                        config.password = args[i + 1].clone();
                        i += 2;
                    } else {
                        return Err("Missing value for --password".into());
                    }
                }
                "--data-dir" => {
                    if i + 1 < args.len() {
                        config.engine.data_dir = PathBuf::from(&args[i + 1]);
                        i += 2;
                    } else {
                        return Err("Missing value for --data-dir".into());
                    }
                }
                "--log-level" => {
                    if i + 1 < args.len() {
                        config.log.level = args[i + 1].clone();
                        i += 2;
                    } else {
                        return Err("Missing value for --log-level".into());
                    }
                }
                "--log-file" => {
                    if i + 1 < args.len() {
                        config.log.file = Some(PathBuf::from(&args[i + 1]));
                        i += 2;
                    } else {
                        return Err("Missing value for --log-file".into());
                    }
                }
                "--config" | "-c" => {
                    if i + 1 < args.len() {
                        let config_path = &args[i + 1];
                        return Self::from_file(config_path);
                    } else {
                        return Err("Missing value for --config".into());
                    }
                }
                "--no-graphql" => {
                    config.graphql_port = None;
                    i += 1;
                }
                "--no-es" => {
                    config.es_port = None;
                    i += 1;
                }
                "--no-mysql" => {
                    config.mysql_port = None;
                    i += 1;
                }
                // Cluster arguments
                "--cluster-id" => {
                    if i + 1 < args.len() {
                        config
                            .cluster
                            .get_or_insert_with(ClusterSettings::default)
                            .cluster_id = args[i + 1].clone();
                        i += 2;
                    } else {
                        return Err("Missing value for --cluster-id".into());
                    }
                }
                "--gossip-port" => {
                    if i + 1 < args.len() {
                        config
                            .cluster
                            .get_or_insert_with(ClusterSettings::default)
                            .gossip_port = args[i + 1].parse()?;
                        i += 2;
                    } else {
                        return Err("Missing value for --gossip-port".into());
                    }
                }
                "--internal-port" => {
                    if i + 1 < args.len() {
                        config
                            .cluster
                            .get_or_insert_with(ClusterSettings::default)
                            .internal_port = args[i + 1].parse()?;
                        i += 2;
                    } else {
                        return Err("Missing value for --internal-port".into());
                    }
                }
                "--seed-nodes" => {
                    if i + 1 < args.len() {
                        config
                            .cluster
                            .get_or_insert_with(ClusterSettings::default)
                            .seed_nodes = args[i + 1]
                            .split(',')
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty())
                            .collect();
                        i += 2;
                    } else {
                        return Err("Missing value for --seed-nodes".into());
                    }
                }
                "--help" => {
                    Self::print_help();
                    std::process::exit(0);
                }
                _ => {
                    log::error!("Unknown argument: {}", args[i]);
                    Self::print_help();
                    return Err(format!("Unknown argument: {}", args[i]).into());
                }
            }
        }

        // 从环境变量覆盖
        if let Ok(host) = std::env::var("CALM_HOST") {
            config.host = host;
        }
        if let Ok(port) = std::env::var("CALM_GRAPHQL_PORT") {
            config.graphql_port = Some(port.parse()?);
        }
        if let Ok(port) = std::env::var("CALM_ES_PORT") {
            config.es_port = Some(port.parse()?);
        }
        if let Ok(port) = std::env::var("CALM_MYSQL_PORT") {
            config.mysql_port = Some(port.parse()?);
        }
        if let Ok(user) = std::env::var("CALM_USER") {
            config.user = user;
        }
        if let Ok(password) = std::env::var("CALM_PASSWORD") {
            config.password = password;
        }
        if let Ok(dir) = std::env::var("CALM_DATA_DIR") {
            config.engine.data_dir = PathBuf::from(dir);
        }
        if let Ok(level) = std::env::var("CALM_LOG_LEVEL") {
            config.log.level = level;
        }
        if let Ok(file) = std::env::var("CALM_LOG_FILE") {
            config.log.file = Some(PathBuf::from(file));
        }

        // Cluster environment variables
        if let Ok(val) = std::env::var("CALM_CLUSTER_ID") {
            config
                .cluster
                .get_or_insert_with(ClusterSettings::default)
                .cluster_id = val;
        }
        if let Ok(val) = std::env::var("CALM_GOSSIP_PORT") {
            config
                .cluster
                .get_or_insert_with(ClusterSettings::default)
                .gossip_port = val.parse()?;
        }
        if let Ok(val) = std::env::var("CALM_INTERNAL_PORT") {
            config
                .cluster
                .get_or_insert_with(ClusterSettings::default)
                .internal_port = val.parse()?;
        }
        if let Ok(val) = std::env::var("CALM_SEED_NODES") {
            config
                .cluster
                .get_or_insert_with(ClusterSettings::default)
                .seed_nodes = val
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }

        Ok(config)
    }

    /// 从文件加载配置
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = if path.ends_with(".toml") {
            toml::from_str(&content)?
        } else if path.ends_with(".json") {
            serde_json::from_str(&content)?
        } else {
            return Err("Unsupported config file format. Use .toml or .json".into());
        };
        Ok(config)
    }

    /// 转换为 EngineConfig
    pub fn to_engine_config(&self) -> EngineConfig {
        EngineConfig {
            data_dir: self.engine.data_dir.clone(),
            persist_check_interval_secs: self.engine.persist_check_interval_secs,
            max_concurrent_persists: self.engine.max_concurrent_persists,
            check_after_flush: self.engine.check_after_flush,
        }
    }

    /// 转换为 ClusterConfig（仅当配置了集群时返回 Some）
    pub fn to_cluster_config(&self) -> Option<ClusterConfig> {
        self.cluster.as_ref().map(|cluster| ClusterConfig {
            cluster_id: cluster.cluster_id.clone(),
            gossip_port: cluster.gossip_port,
            internal_port: cluster.internal_port,
            seed_nodes: cluster.seed_nodes.clone(),
            gossip_interval: Duration::from_millis(cluster.gossip_interval_ms),
            failure_timeout: Duration::from_secs(cluster.failure_timeout_secs),
            suspect_timeout: Duration::from_secs(cluster.suspect_timeout_secs),
        })
    }

    /// 转换为 DistributedConfig（仅当配置了集群时返回 Some）
    // pub fn to_distributed_config(&self) -> Option<DistributedConfig> {
    //     self.cluster.as_ref().map(|cluster| {
    //         let distributed = &cluster.distributed;
    //         DistributedConfig {
    //             query_timeout_ms: distributed.query_timeout_ms,
    //             shuffle_buffer_size: distributed.shuffle_buffer_size,
    //             max_concurrent_queries: distributed.max_concurrent_queries,
    //             rpc_port: distributed.rpc_port,
    //             connect_timeout_ms: distributed.connect_timeout_ms,
    //             request_timeout_ms: distributed.request_timeout_ms,
    //         }
    //     })
    // }

    /// 打印帮助信息
    fn print_help() {
        println!("Calm Database - Multi-Protocol Database Server");
        println!(
            "📦 Version: {} ({})",
            version_macro::build_git_branch!(),
            version_macro::build_git_version!()
        );
        println!("Build time: {}", version_macro::build_time!());
        println!();
        println!("USAGE:");
        println!("    calm [OPTIONS]");
        println!();
        println!("OPTIONS:");
        println!("    -h, --host <HOST>              监听地址 [default: 127.0.0.1]");
        println!("    --graphql-port <PORT>          GraphQL 服务端口 [default: 9567]");
        println!("    --es-port <PORT>               Elasticsearch 服务端口 [default: 9200]");
        println!("    --mysql-port <PORT>            MySQL 服务端口 [default: 3307]");
        println!("    -u, --user <USER>              用户名 [default: root]");
        println!("    -p, --password <PASSWORD>      密码 [default: '']");
        println!("    --data-dir <DIR>               数据目录 [default: ./data]");
        println!("    --log-level <LEVEL>            日志级别 (trace|debug|info|warn|error) [default: info]");
        println!("    --log-file <FILE>              日志文件路径 [default: ./logs/calm.log]");
        println!("    -c, --config <FILE>            配置文件路径 (.toml 或 .json)");
        println!("    --no-graphql                   禁用 GraphQL 服务");
        println!("    --no-es                        禁用 Elasticsearch 服务");
        println!("    --no-mysql                     禁用 MySQL 服务");
        println!("    --help                         显示帮助信息");
        println!();
        println!("CLUSTER OPTIONS:");
        println!("    --node-id <ID>                 节点 ID [default: auto-generated]");
        println!("    --cluster-id <ID>              集群 ID [default: calm-cluster]");
        println!("    --gossip-port <PORT>           Gossip 监听端口 [default: 7946]");
        println!("    --internal-port <PORT>         内部 RPC 服务端口 [default: 7947]");
        println!("    --seed-nodes <NODES>           种子节点列表 (逗号分隔)");
        println!();
        println!("ENVIRONMENT VARIABLES:");
        println!("    CALM_HOST                      监听地址");
        println!("    CALM_GRAPHQL_PORT              GraphQL 服务端口");
        println!("    CALM_ES_PORT                   Elasticsearch 服务端口");
        println!("    CALM_MYSQL_PORT                MySQL 服务端口");
        println!("    CALM_USER                      用户名");
        println!("    CALM_PASSWORD                  密码");
        println!("    CALM_DATA_DIR                  数据目录");
        println!("    CALM_LOG_LEVEL                 日志级别");
        println!("    CALM_LOG_FILE                  日志文件路径");
        println!("    CALM_NODE_ID                   节点 ID");
        println!("    CALM_CLUSTER_ID                集群 ID");
        println!("    CALM_GOSSIP_PORT               Gossip 监听端口");
        println!("    CALM_INTERNAL_PORT             内部 RPC 服务端口");
        println!("    CALM_SEED_NODES                种子节点列表 (逗号分隔)");
        println!();
        println!("EXAMPLES:");
        println!("    # 使用默认配置启动所有服务");
        println!("    calm");
        println!();
        println!("    # 自定义端口");
        println!("    calm --graphql-port 9000 --es-port 9300");
        println!();
        println!("    # 只启动 GraphQL 服务");
        println!("    calm --no-es --no-mysql");
        println!();
        println!("    # 使用配置文件");
        println!("    calm --config config.toml");
        println!();
        println!("    # 启动集群模式");
        println!("    calm --seed-nodes 192.168.1.10:7946,192.168.1.11:7946");
    }
}
