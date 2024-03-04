# Eggstrain

`eggstrain` is an discrete Execution Engine targeting analytical workloads. Written in Rust, `eggstrain` is built on top of and inspired by [DataFusion](https://arrow.apache.org/datafusion/).

`eggstrain` is being built by Connor Tsui, Kyle Booker, and Sarvesh Tandon.


## Architectural Design

`eggstrain` is closely tied to the high-performance asynchronous runtime `tokio`. By relying on `tokio` to manage all dataflow, `eggstrain` is able to offload the complexity of managing dataflow between operators to an asynchronous scheduler, while focusing on high parallel performance.

`eggstrain` is neither a push- nor pull-based Execution Engine in the traditional sense. Since data flow is asynchronous, the `tokio` scheduler gets to decide whether a call to `execute` from a parent operator results in the parent waiting for data to be pushed from the child operator, or if another operator gets to run while the parent operator yields. More specifically, it is possible that data is pushed all the way from the bottom to the top of a pipeline on a single thread (without any contention from other threads), but is is also possible that data gets pushed to a parent operator without the parent operator running with that data for a long time.

The integration with `tokio` channels, `rayon`, and other crates is explained later in [3rd-party Crates](#3rd-party-crates).


## Operators

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


## 3rd-party Crates

`eggstrain` is tightly coupled to `tokio` and `datafusion`. `rayon` and `arrow` are also used heavily in `eggstrain`. The `tracing` crate may be implemented in the future for logging and tracking statistics.

[**`tokio`**](https://tokio.rs/) is a high-performance asynchronous runtime that provides a well-tested work-stealing scheduler over a lightweight thread pool of asynchronous tasks (otherwise known as coroutines). `tokio` is open-source, very well-documented, and has many features and a high degree of community support.

Other than the runtime provided by `tokio::main`, `tokio` provides asynchronous channels that allow data to be sent between threads in a completely thread-safe, efficient manner. `tokio` channels come in 4 flavors, but `eggstrain` only cares about the `oneshot` and `broadcast` (it is possible that in the future, `broadcast` will be replaced with a combination of `mpsc` and an `Exchange` operator). See the [Data Flow](#data-flow) section for more information on how channels are used in `eggstrain`.

One final thing to note is that the `tokio` scheduler has a feature called a LIFO slot. Since `tokio` has a thread pool that takes work off of a shared work queue, it is possible that in a naive implementation, data is sent upwards to a parent task, but the parent task gets placed at the back of the queue, and thus the parent task does not get to run for a "long" time ("long" meaning however long all other queued tasks take to run). If this were to happen, all memory locality would be completely lost, since other tasks may run and replace the pages in the CPU cache _and_ TLB.

The LIFO slot is implemented on `tokio`'s channels to prevent this from happening. When data is passed through a channel, the task on the receiving end of the channel is placed in the LIFO slot. Before worker threads look at the work queue, they will check if there is a task in the LIFO slot, and if there is, they will run that task instead of the first item on the work queue. This reduces the number of cache and TLB misses that might take place if the parent task runs a "long" time after the child task has sent the data upwards. See this [blog](https://tokio.rs/blog/2019-10-scheduler#optimizing-for-message-passing-patterns) from `tokio` for more details.

---

[**`rayon`**](https://docs.rs/rayon/latest/rayon/) is a parallelism crate that allows us to write parallelized and safe code using a OS thread pool with almost no developer cost. As opposed to `tokio`, which uses lightweight tasks (coroutines) which are completely in userspace, `rayon` is much more "heavyweight", as OS threads will incur system calls and other synchronization-related costs.

The reason `eggstrain` needs both `tokio` and `rayon` is subtle, but it comes down to using `tokio` for interacting with anything that might need to wait for a bit of time without doing computation (the storage client from the I/O Service component, disk reads and writes, and even data movement between threads), and using `rayon` for any CPU-intensive workloads (actual execution and computation). Since `tokio` provides the scheduling for `eggstrain`, all `eggstrain` needs to focus on is making operators run as fast as possible.

---

There is a Rust-native [`arrow`](https://arrow.apache.org/rust/arrow/index.html) crate that gives bindings into the [Apache Arrow data format](https://arrow.apache.org/overview/). Notably, it provides a [`RecordBatch`](https://arrow.apache.org/rust/arrow/array/struct.RecordBatch.html) type that can hold vectorized batches of columnar data. This will be the main form of intermediate data communicated between `eggstrain` and other components, as well as between the operators of  `eggstrain` itself.

`eggstrain` is very similar to and makes heavy use of code from [`datafusion`](https://docs.rs/datafusion/latest/datafusion/). `eggstrain` takes as input a DataFusion [`ExecutionPlan`](https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html), and outputs a stream of `RecordBatch`es.

_Note: Since `eggstrain` is using `datafusion`'s physical plan representation, we will make heavy use of their proprietary data types (for now). We will also use the DataFusion implementations for the operators we do not plan to implement ourselves._


## Data Flow

`eggstrain` takes as input a `datafusion` `ExecutionPlan` as input, which is a DAG of physical plan nodes, connected by pointers (`Arc<ExecutionPlan>`).  `eggstrain` mimics this DAG by creating a DAG of operator tasks, connected by `tokio` channels.

Every intermediate operator (non-leaf) runs in its own `tokio` task (lightweight thread). Depending on if the logical node has 1 or 2 children (a unary or binary node), the task will have one or two input channel receivers over the `RecordBatch` type. For non-pipeline breakers, the operator will process `RecordBatch`es immediately, and then send the data up via an input channel sender, which sends the data to the parent operator's task. For pipeline breaker such as `Sort` or `HashBuild`, data is gathered until all input data has been sent from the child, and from there all data is processed together.


### `Project` Example

Here is a diagram of the `Project` operator, an intermediate operator that is _not_ a pipeline breaker.

```text
                            ▲
                            │
                            │
                            │
                            │ RecordBatch
   Project Operator Task    │
                            │
            ┌──────────┬────┴──────┬────────────┐
            │          │           │            │
            │          │    tx     │            │
            │          │           │            │
            │          └────▲──────┘            │
            │               │                   │
            │                                   │
            │       project(record_batch);      │
            │               ▲                   │
            │               │                   │
            │          ┌────┴──────┐            │
            │          │           │            │
            │          │    rx     │            │
            │          │           │            │
            └──────────┴────▲──────┴────────────┘
                            │
                            │
                            │
                            │ RecordBatch
                            │
                            │
                            │
```

Since `Project` is not a pipeline breaker, it will apply a projection to every `RecordBatch` it encounters from `rx`, and then immediately send it to its parent via `tx`.


### `Sort` Example

Here is a diagram of the `Sort` operator, which _is_ a pipeline breaker (as it needs all input data before it can sort anything).

```text
                                                         │
                                                         │
                        ▲             Asynchronous Land  │  Synchronous Land
                        │                  (tokio)       │      (rayon)
                        │                                │
                        │ RecordBatch                    │
 Sort Operator Task     │                                │
                        │                                │
┌──────────────────┬────┴─────┬──────────────────────┐   │
│                  │          │                      │   │
│                  │    tx    │                      │   │
│                  │          │                      │   │
│                  └────▲─────┘                      │   │   Rayon Thread
│                       │              ┌─────────────┤   │  ┌─────────────┐
│                       │              │             │   │  │             │
│                       └──────────────┤ oneshot::rx ◄───┼──┤ oneshot::tx │
│                                      │             │   │  │             │
│                                      └─────────────┤   │  ├──────▲──────┤
│         Temporary Buffer                           │   │  │      │      │
│   ┌───────────────────────────┐                    │   │  │      │      │
│   │                           │                    │   │  │      │      │
│   │  Buffer of RecordBatches  │   rayon::spawn();  │   │  │             │
│   │                           ├────────────────────┼───┼──► sort(data); │
│   │   Stored in memory and    │                    │   │  │             │
│   │     spilled to disk       │                    │   │  └─────────────┘
│   │                           │                    │   │
│   └─────────────▲─────────────┘                    │   │
│                 │                                  │   │
│          ┌──────┘                                  │   │
│          │                                         │   │
│     ┌────┴─────┐                                   │   │
│     │          │                                   │   │
│     │    rx    │                                   │   │
│     │          │                                   │   │
└─────┴────▲─────┴───────────────────────────────────┘   │
           │                                             │
           │                                             │
           │ RecordBatch                                 │
           │                                             │
           │
```


`Sort` will continuously poll its child task by calling `rx.recv()` on the input channel, until it returns a `RecvError::Closed`. All `RecordBatch`es are stored in some temporary buffer, either completely in memory, or with some contents spilled to temporary files on disk. See the [async I/O](#asynchronous-io-and-buffer-pool-manager) section for more details.

Once all data has been buffered, the `Sort` operator can begin the sorting phase. Instead of computing the computationally heavy sort algorithm in the `tokio` task, `Sort` will instead spawn a `rayon`-managed OS thread to offload the computational workload to a more "heavyweight" worker. Since `tokio` is a completely userspace thread library, this is incredibly important, since work done by a `Sort` operator within a `tokio` task would cause other `tokio` tasks to block on the `sort` computation. By instead sending the data to a synchronous OS thread, other lightweight tasks do not need to block on the `sort` computation, and the CPU running those threads can choose to run something else.

This is only possible via the `oneshot::channel`, which provides a synchronous method `tx.send()` to send data from a synchronous thread to an asynchronous task. On the synchronous side, the OS thread only needs to call `tx.send(sorted_data)`, while the asynchronous task only needs to call `rx.await`. This is key, since the `.await` call will allow the `tokio` scheduler to run other tasks while the `sort` is running.

Once the `rx.await` call returns with the sorted data, `Sort` can send data up to the next operator in fixed-sized batches.

<br>
<br>
<br>

# In Progress Sections

All of these sections are incomplete, but give some insight into our plans for the future.

## Asynchronous I/O and Buffer Pool Manager

Since `eggstrain` is so tightly coupled with an asynchronous runtime, it makes sense to use that to our benefit. Typically, we have to wait for disk reads and writes to finish before we can get access to a memory page. In synchronous land, there is nothing that we can do but yield while the OS runs a system call. The OS is allowed to context switch while this disk I/O is happening, meaning the read/write might take a "long" time to return.

It is very possible we could see large performance benefits from using asynchronous I/O for a buffer pool manager. More specifically, we could implement a buffer pool manager that has both synchronous (blocking) and asynchronous methods for reading and writing, which would give as plenty of flexibility in when we want to use one or the other.

This would probably take more time, but it might also be a great contribution we could make, since as of right now, our individual operators will never be able to outperform `datafusion`.


## Interface and API

`eggstrain` can be treated as a mathematical function. It receives as input an `ExecutionPlan` physical plan, and outputs a stream of Apache Arrow `RecordBatch`es, or a `SendableRecordBatchStream`.

The `ExecutionPlan` that `eggstrain` receives as input has nodes corresponding to one of the operators listed above. These plans will be given to each `eggstrain` instance by a Scheduler / Coordinator as DataFusion query plan fragments. More specifically, `eggstrain` will parse out the specific nodes in the relational DAG, and construct its own DAG of operators, connected by channels instead of pointers.

Once `eggstrain` parses the plan, it will figure out what data it requires. From there, it will make a high-level request for data it needs from the IO Service (e.g. logical columns from a table).

TODO something about `TableScan`, need to talk more with the I/O teams.
