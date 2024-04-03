use arrow::util::pretty;
use datafusion::sql::sqlparser::ast::Query;
use datafusion_common::Result;
use eggstrain::{run, tpch_ctx};
use std::io;

#[tokio::main]
async fn main() -> Result<()> {
    // Create a SessionContext with TPCH base tables
    let ctx = tpch_ctx().await?;

    // Create a DataFrame with the input query
    let query = io::read_to_string(io::stdin())?;
    let sql = ctx.sql(&query).await?;

    // Run our execution engine on the physical plan
    let df_physical_plan = sql.clone().create_physical_plan().await?;
    println!("{:#?}", df_physical_plan.clone());
    let df_physical_plan = df_physical_plan.children()[0].clone();
    // let df_physical_plan = df_physical_plan.children()[0].clone();
    let results = run(df_physical_plan).await;

    results.into_iter().for_each(|batch| {
        let pretty_results = pretty::pretty_format_batches(&[batch]).unwrap().to_string();
        println!("{}", pretty_results);
    });

    Ok(())
}
