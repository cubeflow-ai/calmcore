/// 测试 DDL 宏生成的代码
///
/// 运行方式：
/// ```bash
/// cargo expand --example test_ddl_macro
/// ```
use ddl_macros::coordinator_route;

struct TestService {
    is_coord: bool,
}

impl TestService {
    fn am_i_coord_node(&self) -> bool {
        self.is_coord
    }

    async fn coord_client(&self) -> Result<(), String> {
        Ok(())
    }
}

#[tarpc::service]
trait TestDDL {
    async fn create_table(name: String, schema: String) -> Result<(), String>;
}

impl TestDDL for TestService {
    #[coordinator_route]
    async fn create_table(
        self,
        _ctx: tarpc::context::Context,
        name: String,
        schema: String,
    ) -> Result<(), String> {
        // 协调者逻辑
        println!("Creating table {} with schema {}", name, schema);
        Ok(())
    }
}

fn main() {
    println!("This is a macro expansion test example.");
    println!("Run: cargo expand --example test_ddl_macro");
}
