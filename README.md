# Eggstrain

`eggstrain` is an discrete Execution Engine targeting analytical workloads. Written in Rust, `eggstrain` is built on top of and inspired by [DataFusion](https://arrow.apache.org/datafusion/).

`eggstrain` is being built by Connor Tsui, Kyle Booker, and Sarvesh Tandon.


## Architectural Design

`eggstrain` is closely tied to the high-performance asynchronous runtime `tokio`, which implements a work-stealing scheduler. By relying on `tokio` to manage all dataflow, `eggstrain` is able to offload the complexity of managing dataflow between operators to an asynchronous scheduler, while focusing on high parallel performance by leveraging the `rayon` crate.

`eggstrain` is neither a push- nor pull-based Execution Engine in the traditional sense. Since data flow is asynchronous, the `tokio` scheduler gets to decide whether a call to `execute` from a parent operator results in the parent waiting for data to be pushed from the child operator, or if another operator gets to run while the parent operator yields. More specifically, it is possible that data is pushed from the bottom to the top of a pipeline on a single thread, without any interference, but is is also possible that data gets pushed to a parent operator without the parent operator running for a long time.

The integration with `tokio` channels, `rayon`, and other crates is explained later in [3rd-party Crates](#3rd-party-crates).

---

### Operators

`eggstrain` supports a few operators, and there are more to come.

- `TableScan`
- `Filter` (Completed)
- `Project` (Completed)
- `HashAggregate` (in progress)
- `HashProbe` + `HashBuild` (in progress)
- `MergeJoin`
- `Sort` (Completed)
- `Exchange`

All intermediate operators (non-leaf operators) must implement either the `UnaryOperator` or `BinaryOperator` trait, depending on the number of logical children an operator has. Special nodes like `TableScan` and `Exchange` are dealt with separately. Both of these traits rely on the `async-trait` crate, as asynchronous sendable trait objects are not stable as of Rust 1.76.

---

### 3rd-party Crates

`eggstrain` makes heavy use of `tokio` and `rayon`. The `tracing` crate may be implemented in the future for logging and tracking statistics.

[**`tokio`**](https://tokio.rs/) is a high-performance asynchronous runtime that provides a well-tested work-stealing scheduler over a lightweight thread pool of asynchronous tasks (otherwise known as coroutines). `tokio` is open-source, very well-documented, and has many features and a high degree of support.

Other than the runtime provided by `tokio::main`, `tokio` provides asynchronous channels that allow data to be sent between threads in a completely thread-safe, efficient manner. `tokio` channels come in 4 flavors, but `eggstrain` only cares about the `oneshot` and `broadcast` (it is possible that in the future, `broadcast` will be replaced with a combination of `mpsc` and an `Exchange` operator). See the [Data Flow](#data-flow) section for more information on how channels are used in `eggstrain`.

One final thing to note is that the `tokio` scheduler has a feature called a LIFO slot. Since `tokio` has a thread pool that takes work off of a shared work queue, it is possible that in a naive implementation, data is sent to a parent operator, but the parent operator gets placed at the back of the queue, and thus the parent operator does not get to run for a "long" time. If this were to happen, all memory locality would be completely lost, since other operators may run and replace the pages in the CPU cache _and_ TLB.

The LIFO slot is included to prevent this from happening. When data is passed through a channel, the task on the receiving channel is placed in the LIFO slot. Before worker threads look at the work queue, they will check if there is a task in the LIFO slot, and if there is, they will run that task instead of the first item on the work queue. This reduces the number of cache and TLB misses that might take place if the parent task runs a "long" time after the child task has sent the data upwards. See this [blog](https://tokio.rs/blog/2019-10-scheduler#optimizing-for-message-passing-patterns) from `tokio` for more details.

---

[**`rayon`**](https://docs.rs/rayon/latest/rayon/) is a parallelism crate that allows us to write parallelized and safe code using a OS thread pool with almost no developer cost. As opposed to `tokio`, which uses lightweight tasks (coroutines) which are completely in userspace, `rayon` is much more "heavyweight", as OS threads will incur system calls and other synchronization-related costs.

The reason `eggstrain` needs both `tokio` and `rayon` is subtle, but it comes down to using `tokio` for interacting with anything that might need to wait for a bit of time without doing computation (the storage client from the I/O Service component, disk reads and writes, and even data movement between threads), and using `rayon` for any CPU-intensive workloads (actual execution and computation). Since `tokio` provides the scheduling for `eggstrain`, all `eggstrain` needs to focus on is making operators run as fast as possible by leveraging `rayon`.

---

There is a Rust-native [`arrow`](https://arrow.apache.org/rust/arrow/index.html) crate that gives bindings into the [Apache Arrow data format](https://arrow.apache.org/overview/). Notably, it provides a [`RecordBatch`](https://arrow.apache.org/rust/arrow/array/struct.RecordBatch.html) type that can hold vectorized batches of columnar data. This will be the main form of intermediate data communicated between `eggstrain` and other components, as well as between the operators of the EE itself.

`eggstrain` is very similar to and makes heavy use of code from [`datafusion`](https://docs.rs/datafusion/latest/datafusion/). `eggstrain` takes as input a DataFusion [`ExecutionPlan`](https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html), and outputs a stream of `RecordBatch`es.

_Note: Since we are using their physical plan representation we will make heavy use of their proprietary data types. We will also use the DataFusion implementations for the operators we do not plan to implement ourselves for the time being._

---

### Data Flow

TODO ASCII diagram


### Interface and API

`eggstrain` can be treated as a mathematical function. It receives as input an `ExecutionPlan` physical plan from the scheduler, and outputs a stream of Apache Arrow `RecordBatch`es, or a `SendableRecordBatchStream`.

The `ExecutionPlan` that `eggstrain` receives as input has nodes corresponding to one of the operators listed above. These plans will be given to each `eggstrain` instance by a Scheduler / Coordinator as DataFusion query plan fragments. More specifically, `eggstrain` will parse out the specific nodes in the relational DAG, and construct its own DAG of operators, connected by channels instead of pointers.

Once `eggstrain` parses the plan, it will figure out what data it requires. From there, it will make a high-level request for data it needs from the IO Service (e.g. logical columns from a table).

TODO something about `TableScan`

---

### Other Sections

TODO
