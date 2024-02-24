use arrow::util::pretty;
use datafusion_common::Result;
use eggstrain::{run, tpch_dataframe};

#[tokio::main]
async fn main() -> Result<()> {
    let tpch = tpch_dataframe().await?;

    let physical_plan = tpch.clone().create_physical_plan().await?;

    let results = run(physical_plan).await;

    results.into_iter().for_each(|batch| {
        let pretty_results = pretty::pretty_format_batches(&[batch]).unwrap().to_string();
        println!("{}", pretty_results);
    });

    Ok(())
}
