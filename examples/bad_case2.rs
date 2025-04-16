use calmcore::util::CoreResult;
use calmcore::CalmCore;

fn main() -> CoreResult<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug")).init();

    let data_path = "/Users/sunjian/rustworkspace/search-benchmark-game/engines/calmcore-0.1.0/idx";
    let schema_name = "calmcore-bench";

    let core = CalmCore::new(data_path)?;

    let space = core.load_engine(schema_name)?;
    let result =
        space.sql("select id, text from test where text=text('he was elected under indian national', operator='and') order by _score asc ")?;
    println!("sql result:{:?}", result.to_wrapper());

    Ok(())
}
