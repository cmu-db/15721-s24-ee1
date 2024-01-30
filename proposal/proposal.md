# Execution Engine

* Sarvesh (sarvesht)
* Kyle (kbooker)
* Connor (cjtsui)



# Overview
> What is the goal of this project? What will this component achieve?

The purpose of this project is to create the Execution Engine (EE) for a distributed OLAP database.

We will be taking heavy inspiration from [DataFusion](https://arrow.apache.org/datafusion/), [Velox](https://velox-lib.io/), and [InfluxDB](https://github.com/influxdata/influxdb) (which itself is built on top of DataFusion).

There are two subgoals. The first is to develop a functional EE, with a sufficient number of operators working and tested. Since all other components will have to rely on us to see any sort of outputs, it would be ideal if we had naive implementations of operators that just work (even if they are slow).

The second is to add either interesting features or optimize the engine to be more performant (or both). Since it is unlikely that we will outperform any off-the-shelf EEs like DataFusion, we will likely try to test some new feature that these engines do not use themselves.



# Architectural Design
> Explain the input and output of the component, describe interactions and breakdown the smaller components if any. Include diagrams if appropriate.

We will be creating a vectorized push-based EE. This means operators will push batches of data up to their parent operators in the physical plan tree.

---

### Operators

We will implement a subset of the operators that [Velox implements](https://facebookincubator.github.io/velox/develop/operators.html):
- TableScan
- FilterProject
- HashAggregation
- HashProbe + HashBuild
- MergeJoin
- NestedLoopJoin
- OrderBy
- TopN
- Limit
- Values

The `trait` / interface to define these operators is unknown right now. We will likely follow whatever DataFusion is outputting from their [`ExecutionPlan::execute()`](https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html#tymethod.execute) methods.

---

### 3rd-party crates

We will be making heavy use of `tokio` and `rayon`.

[`tokio`](https://tokio.rs/) is a high-performance asynchronous runtime for Rust that provides a very well-tested work-stealing lightweight thread pool that is also very well-documented with lots of features and support.

[`rayon`](https://docs.rs/rayon/latest/rayon/) is a data parallelism crate that allows us to write parallelized and safe code using a OS thread pool with almost no developer cost.

The difference is a bit subtle, but it comes down to using `tokio` for interacting with the storage client from the I/O Service component (basically handling anything asynchronous), and using `rayon` for any CPU-intensive workloads (actual execution). `tokio` will provide the scheduling for us, and all we will need to do is write the executors that `tokio` will run for us.

There is a Rust-native [`arrow`](https://arrow.apache.org/rust/arrow/index.html) crate that gives us bindings into the [Apache Arrow data format](https://arrow.apache.org/overview/). Notably, it provides a [`RecordBatch`](https://arrow.apache.org/rust/arrow/array/struct.RecordBatch.html) type that can hold vectorized batches of columnar data. This will be the main form of data communicated between the EE and other components, as well as between the operators of the EE itself.

Finally, there is a [`substrait`](https://docs.rs/substrait/latest/substrait/) crate that provides a way to decode query plan fragments encoded in the substrait format.

---

### Interface and API

Each EE can be treated as a mathematical function. It receives as input a physical plan from the scheduler, and outputs an Apache Arrow `RecordBatch` or `SendableRecordBatchStream`.

The physical plan tree it receives as input has nodes corresponding to one of the operators listed above. These plans will be given to each EE by the Scheduler / Coordinator as [substrait](https://substrait.io/) query plan fragments. More specifically, the EE will parse out the specific nodes in the relational tree, nodes represented in this [`substrait::proto::rel::RelType` enum](https://docs.rs/substrait/latest/substrait/proto/rel/enum.RelType.html).

```rust
pub enum RelType {
    Read(Box<ReadRel>),
    Filter(Box<FilterRel>),
    Fetch(Box<FetchRel>),
    Aggregate(Box<AggregateRel>),
    Sort(Box<SortRel>),
    Join(Box<JoinRel>),
    Project(Box<ProjectRel>),
    Set(SetRel),
    ExtensionSingle(Box<ExtensionSingleRel>),
    ExtensionMulti(ExtensionMultiRel),
    ExtensionLeaf(ExtensionLeafRel),
    Cross(Box<CrossRel>),
    Reference(ReferenceRel),
    Write(Box<WriteRel>),
    Ddl(Box<DdlRel>),
    HashJoin(Box<HashJoinRel>),
    MergeJoin(Box<MergeJoinRel>),
    NestedLoopJoin(Box<NestedLoopJoinRel>),
    Window(Box<ConsistentPartitionWindowRel>),
    Exchange(Box<ExchangeRel>),
    Expand(Box<ExpandRel>),
}
```

Once the EE parses the plan and will figure out what data it requires. From there, it will make a high-level request for data it needs from the IO Service (e.g. logical columns from a table). For the first stages of this project, we will just request entire columns of data from specific tables. Later in the semester, we may want more granular columnar data, or even point lookups.

The IO Service will have a `StorageClient` type that the EE can use locally on an execution node as a way to abstract the data it is requesting and receiving (_the IO service team will expose this type from their own crate, and we can link it into our EE crate_). The IO Service will return to the EE data as a `tokio::Stream` of Apache Arrow `RecordBatch` (something like `Stream<Item = RecordBatch>`, which will probably mean using a re-exported [`SendableRecordBatchStream`](https://docs.rs/datafusion/latest/datafusion/physical_plan/type.SendableRecordBatchStream.html) from DataFusion).

The IO Service will retrieve the data (presumably by talking with the Catalog) from the blob store, parse it into an Apache Arrow format (which means the IO service is in charge of decoding Parquet files into the Arrow format), and then send it to the EE as some form of asynchronous stream.

A potential `StorageClient` API in Rust that the IO service would expose:

```rust
//! Right now we have this in a submodule `storage_client.rs`, but the IO service 
//! team would probably create a crate and we could import it easily into our `Cargo.toml` file

use anyhow::Result;
use arrow_array::RecordBatch;
use tokio_stream::Stream;

// Placeholder types to let this compile
type ColumnId = String;
type TableId = String;
type RecordId = usize;

/// For now, pretend that this is an opaque type that the
/// I/O Service team will provide to us in a crate.
/// This type should be `Sync` as well, to support
/// multiple instances of a `StorageClient`.
pub struct StorageClient;

/// Have some way to request specific types of data.
/// As long as it comes back as a `RecordBatch`,
/// we should be fine to have any type of request here.
pub enum BlobData {
    Table(TableId),
    Columns(TableId, Box<[ColumnId]>),
    Tuple(RecordId),
}

impl StorageClient {
    /// Have some sort of way to create a `StorageClient` on our local node.
    pub fn new(_id: usize) -> Self {
        Self
    }

    /// The only other function we need exposed would be a way to actually get data.
    /// What we should get is a stream of `Recordbatch`s, which is just Apache Arrow
    /// data in memory.
    ///
    /// The executor node really should not know what the underlying data is on the Blob data store.
    /// In our case it is Parquet, but since the Execution Engine is not in charge or loading
    /// those Parquet files, it should just receive it as in-memory Arrow data
    ///
    /// Note that we will likely re-export the `SendableRecordBatchRecord` from DataFusion
    /// and use that as the return type instead
    pub async fn request_data(
        &self,
        _request: BlobData,
    ) -> Result<Box<dyn Stream<Item = RecordBatch>>> {
        todo!()
    }
}
```

Plus a usage example  of the `StorageClient`:

```rust
//! Example `main` function for the EE teams

use testing_721::operators;
use testing_721::operators::Operator;
use testing_721::storage_client;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize a storage client
    let sc = storage_client::StorageClient::new(42);

    // Formualte a request we want to make to the storage client
    let request = create_column_request();

    // Request data from the storage client. Note that this request could fail
    let stream = sc.request_data(request).await?;

    // Theoretically, there could be a pipeline breaker somehwere that turns the asynchronous
    // flow into a synchronous one, turning the Stream into an Iterator

    // Executor node returns a future containing another stream that can be sent to another operator
    let table_scan_node = operators::TableScan::new();
    let result = table_scan_node.execute_with_stream(stream);

    Ok(())
}

/// Just formulate a toy example of a request we could make to the `StorageClient`
fn create_column_request() -> storage_client::BlobData {
    let columns = vec!["grades".to_string(), "name".to_string(), "gpa".to_string()];
    storage_client::BlobData::Columns("students".to_string(), columns.into_boxed_slice())
}
```


Once the EE receives the stream, it has all the information it needs, and can start executing the physical plan from the bottom, pushing the output of operators to the next operator. This way, `tokio` does necessarily have to wait for multiple operators to finish before starting the next level's operator. The execution nodes themselves will be on their own dedicated threads (either `tokio` threads that call into `rayon`, but do not make any IO and `async` calls, or OS threads).

The operator interface will probably end up being something similar to how DataFusion outputs data from their [`ExecutionPlan::execute()`](https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html#tymethod.execute) method.

After finishing execution, the EE can notify the scheduler that it has finished executing the query fragment, and it can send the data (in the form of Apache Arrow data) to wherever the scheduler needs it to be sent. This could be to another EE node, the Catalog, or even the Optimizer.

---

### Implementation Details

For our group, data between operators will also be sent and processed through Apache Arrow `RecordBatch` types. This is not how Velox does it, but it is how DataFusion does processing.

It is likely that we will also need our own Buffer Pool Manager to manage in-memory data that needs to be spilled to disk in the case that the amount of data we need to handle grows too much. We may just take the [`memory_pool`](https://docs.rs/datafusion/latest/datafusion/execution/memory_pool/index.html) module out of Datafusion.



# Design Rationale
> Explain the goals of this design and how the design achieves these goals. Present alternatives considered and document why they are not chosen.

We will be focusing on the robustness and modularity of our code to enable future extensibility. Given the inherent modular complexity of a database system, it is ideal to be able to easily switch parts of the system in and out without much developer overhead. By focusing design on an easy-to-use and expressive type system for the EE, future work can be done on optimization without having to rewrite a bunch of boilerplate or interfacing code.



# Testing Plan
> How should the component be tested?

We will add unit testing and documentation testing to every function. Since it is quite difficult to create undefined behavior in Rust, testing will focus on logical correctness and performance.

The integration test will be TPC-H, or something similar to TPC-H. This is a stretch goal.



# Trade-offs and Potential Problems
> Write down any conscious trade-off you made that can be problematic in the future, or any problems discovered during the design process that remain unaddressed (technical debts).


- Push-based vs Pull-based



# Glossary
> If you are introducing new concepts or giving unintuitive names to components, write them down here.

- "Vectorized execution" is the name given to the concept of outputting batches of data. But since there is a `Vec`tor type in Rust, we'll likely be calling everything Batches instead of Vectors.




















