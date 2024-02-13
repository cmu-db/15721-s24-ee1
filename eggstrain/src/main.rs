pub mod execution;
pub mod scheduler_client;
pub mod storage_client;

use execution::substrait::deserialize::*;

#[tokio::main]
async fn main() {
    println!("Hello, world!");
    // get_plan("../substrait/substrait_plan_example.json");
    // get_json("../substrait/basic_query.json");
    get_read("../substrait/read_rel_example.json");
}
