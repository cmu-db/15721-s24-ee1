pub mod execution;
pub mod scheduler_client;
pub mod storage_client;

use execution::substrait::deserialize::get_json;

#[tokio::main]
async fn main() {
    println!("Hello, world!");
    get_json("../substrait/substrait_plan_example.json");
    // get_json("../substrait/basic_query.json");
}
