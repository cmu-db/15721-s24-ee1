# Execution Engine

* Sarvesh (sarvesht)
* Kyle (kbooker)
* Connor (cjtsui)


# Overview

The purpose of this project was to create the Execution Engine (EE) for a distributed OLAP database.

We took heavy inspiration from [DataFusion](https://arrow.apache.org/datafusion/), [Velox](https://velox-lib.io/), and [InfluxDB](https://github.com/influxdata/influxdb) (which itself is built on top of DataFusion).

There were two subgoals. The first is to develop a functional EE, with a sufficient number of operators working and tested. Since all other components have to rely on us to see any sort of outputs, we had have implementations of operators that just work (even though some are inefficient).

The second was to add either interesting features or optimize the engine to be more performant (or both). Since it is unlikely that we will outperform any off-the-shelf EEs like DataFusion, we will likely try to test some new feature that these engines do not use themselves.



# Architectural Design
> Explain the input and output of the component, describe interactions and breakdown the smaller components if any. Include diagrams if appropriate.

We created a vectorized push-based EE. This means operators will push batches of data up to their parent operators in the physical plan tree.

---

### Operators

We implemented a subset of the operators that [Velox implements](https://facebookincubator.github.io/velox/develop/operators.html):
- TableScan (Used Datafusion)
- Filter (Completed)
- Project (Completed)
- HashAggregation (Completed)
- HashProbe + HashBuild (Used Datafusion)
- OrderBy (Completed)
- TopN (Completed)

The `trait` / interface to define these operators is unknown right now. We will likely follow whatever DataFusion is outputting from their [`ExecutionPlan::execute()`](https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html#tymethod.execute) methods.

---

### 3rd-party crates

We will be making heavy use of `tokio` and `rayon`.

[`tokio`](https://tokio.rs/) is a high-performance asynchronous runtime for Rust that provides a very well-tested work-stealing lightweight thread pool that is also very well-documented with lots of features and support.

[`rayon`](https://docs.rs/rayon/latest/rayon/) is a data parallelism crate that allows us to write parallelized and safe code using a OS thread pool with almost no developer cost.

The difference is a bit subtle, but it comes down to using `tokio` for interacting with the storage client from the I/O Service component (basically handling anything asynchronous), and using `rayon` for any CPU-intensive workloads (actual execution). `tokio` will provide the scheduling for us, and all we will need to do is write the executors that `tokio` will run for us.

There is a Rust-native [`arrow`](https://arrow.apache.org/rust/arrow/index.html) crate that gives us bindings into the [Apache Arrow data format](https://arrow.apache.org/overview/). Notably, it provides a [`RecordBatch`](https://arrow.apache.org/rust/arrow/array/struct.RecordBatch.html) type that can hold vectorized batches of columnar data. This will be the main form of data communicated between the EE and other components, as well as between the operators of the EE itself.

We will also be making heavy use of [`datafusion`](https://docs.rs/datafusion/latest/datafusion/). Since we are using their physical plan representation we will make heavy use of their proprietary datatypes. We will also use the DataFusion implementations for the operators we do not plan to implement ourselves for the time being.

---

### Interface and API

Each EE can be treated as a mathematical function. It receives as input a physical plan from the scheduler, and outputs an Apache Arrow `RecordBatch` or `SendableRecordBatchStream`.

The physical plan tree it receives as input has nodes corresponding to one of the operators listed above. These plans will be given to each EE by the Scheduler / Coordinator as DataFusion query plan fragments. More specifically, the EE will parse out the specific nodes in the relational tree.

Once the EE parses the plan and will figure out what data it requires. From there, it will make a high-level request for data it needs from the IO Service (e.g. logical columns from a table). For the first stages of this project, we will just request entire columns of data from specific tables. Later in the semester, we may want more granular columnar data, or even point lookups.

The IO Service will have a `StorageClient` type that the EE can use locally on an execution node as a way to abstract the data it is requesting and receiving (_the IO service team will expose this type from their own crate, and we can link it into our EE crate_). The IO Service will return to the EE data as a `tokio::Stream` of Apache Arrow `RecordBatch` (something like `Stream<Item = RecordBatch>`, which will probably mean using a re-exported [`SendableRecordBatchStream`](https://docs.rs/datafusion/latest/datafusion/physical_plan/type.SendableRecordBatchStream.html) from DataFusion).

The IO Service will retrieve the data (presumably by talking with the Catalog) from the blob store, parse it into an Apache Arrow format (which means the IO service is in charge of decoding Parquet files into the Arrow format), and then send it to the EE as some form of asynchronous stream.

### Implementation Details

For our group, data between operators will also be sent and processed through Apache Arrow `RecordBatch` types. This is not how Velox does it, but it is how DataFusion does processing.

It is likely that we also needed our own Buffer Pool Manager to manage in-memory data that needs to be spilled to disk in the case that the amount of data we need to handle grows too much.

The buffer pool manager in Datafusion was not asynchronous. So in order to fully exploit the advantages of the tokio asynchronous runtime, we shifted focus completely in the last 4 weeks to build out an asynchronous buffer pool manager similar to Leanstore.

# Testing Plan For In-Memory Execution Engine

> How should the component be tested?
The integration test were TPC-H, or something similar to TPC-H. This was a stretch goal. We have completed this and the results of running TPC-H query 1 with scale factor=10 are shown in the final presentation.

# Glossary

> If you are introducing new concepts or giving unintuitive names to components, write them down here.
-   "Vectorized execution" is the name given to the concept of outputting batches of data. But since there is a `Vec`tor type in Rust, we'll likely be calling everything Batches instead of Vectors.

---

<br>
<br>
<br>
<br>

# **Asynchronous Buffer Pool**

_Note: This design documentation for the asynchronous buffer pool is slightly outdated, but the_
_high-level components are still the same. The only real difference is in the eviction algorithm._

For the real documentation, see the up-to-date repository
[here](https://github.com/Connortsui20/async-bpm).

After cloning the repository, run this command to generate the documentation:

```sh
$ cargo doc --document-private-items --open
```

# Design

This model is aimed at a thread-per-core model with a single logical disk.
This implies that tasks (coroutines) given to worker threads cannot be moved between threads
and future work could introduce the all-to-all model of threads to distinct SSDs,
where each worker thread has a dedicated `io_uring` instance for every physical SSD.

# Future Work

There is still a lot of work to be done on this system. As of right now, it is in a state of
"barely working". However, in this "barely working" state, it still matches and even outperforms
RocksDB in IOPS on single-disk hardware. Even though this is not a very high bare, it shows the high
potential of this system, especially since the goal is to scale with better hardware.

Almost all of the [issues](https://github.com/Connortsui20/async-bpm/issues) are geared towards
optimization, and it is not an overstatement to say that each of these features would contribute
to a significant performance gain.

# Objects and Types

## Thread Locals

-   `PageHandle`: A shared pointer to a `Page` (through an `Arc`)
-   Local `io_uring` instances (that are `!Send`)
-   Futures stored in a local hash table defining the lifecycle of an `io_uring` event (private)

### Local Daemons

These thread-local daemons exist as foreground tasks, just like any other task the DBMS might have.

-   Listener: Dedicated to polling local `io_uring` completion events
-   Submitter: Dedicated to submitting `io_uring` submission entries
-   Evictor: Dedicated to cooling `Hot` pages and evicting `Cool` pages

## Shared Objects

-   Shared pre-registered buffers / frames
    -   Frames are owned types (can only belong to a specific `Page`)
    -   Frames also have pointers back to their parent `Page`s
    -   _Will have to register multiple sets, as you can only register 1024 frames at a time_
-   Shared multi-producer multi-consumer channel of frames
-   `Page`: A hybrid-latched (read-write locked for now) page header
    -   State is either `Unloaded`, `Loading` (private), or `Loaded`
        -   `Unloaded` implies that the data is not in memory
        -   `Loading` implies that the data is being loaded from disk, and contains a future (private)
        -   `Loaded` implies the data is on one of the pre-registered buffers, and owns a registered buffer
    -   `Page`s also have eviction state
        -   `Hot` implies that this is a frequently-accessed page
        -   `Cool` implies that it is not frequently accessed, and might be evicted soon

Note that the eviction state is really just an optimization for making a decision on pages to evict.
The page eviction state _does not_ imply anything about the state of a page
(so a page could be `Hot` and also `Unloaded`), and all accesses to a page must still go through the hybrid latch.

In summary, the possible states that the `Page` can be in is:

-   `Loaded` and `Hot` (frequently accessed)
-   `Loaded` and `Cool` (potential candidate for eviction)
-   `Loading` (`Hot`/`Cold` plus private `io_uring` event state)
-   `Unloaded` (`Cold`)

# Algorithms

### Write Access Algorithm

Let P1 be the page we want to get write access for.

-   Set eviction state to `Hot`
-   Write-lock P1
-   If `Loaded` (SEMI-HOT PATH):
    -   Modify the page data
    -   Unlock and return
-   Else `Unloaded`, and we need to load the page
    -   Load a page via the [load algorithm](#load-algorithm)
    -   Modify the page data
    -   Unlock and return

### Read Access Algorithm

Let P1 be the page we want to get read access for.
All optimistic reads have to be done through a read closure (cannot construct a reference `&`).

-   Set eviction state to `Hot`
-   Optimistically read P1
-   If `Loaded` (HOT PATH):
    -   Read the page data optimistically and return
    -   If the optimistic read fails, fallback to a pessimistic read
        -   If still `Loaded` (SEMI-HOT PATH):
            -   Read normally and return
        -   Else it is now `Unloaded`, so continue
-   The state is `Unloaded`, and we need to load a page
    -   Upgrade the read lock into a write lock (either drop and retake, or directly upgrade)
    -   Load a page via the [load algorithm](#load-algorithm)
    -   Read the page data
    -   Unlock and return

### Load algorithm

Let P1 be the page we want to load from disk into memory. The caller must have the write lock on P1.
Once this algorithm is complete, the page is guaranteed to be loaded into the owned frame,
and the page eviction state will be `Hot`.

-   If the page is `Loaded`, then immediately return
-   Otherwise, this page is `Unloaded`
-   pick a random `FrameGroup` from the set of frame groups that we have
-   run the cooling (clock) algorithm on all the frames in the `FrameGroup` until we have a free frame available
-   Set the frame's parent pointer to P1
-   Read P1's data from disk into the buffer
-   `await` read completion from the local `io_uring` instance
-   At the end, set the page eviction state to `Hot`

### General Eviction Algorithm

On every worker thread, if the random `FrameGroup` that it picks does not have a frame,
it will start acting like an evictor and will start running the clock algorithm.
It will aim to have some certain threshold of free pages in the free list.

-   Iterate over all frames in the `FrameGroup`
-   Collect the list of `Page`s that are `Loaded` (should not be more than the number of frames)
-   For every `Page` that is `Hot`, change to `Cool`
-   Collect all `Cool` pages
-   Randomly choose some (small) constant number of pages (current this constant number is 1 but in the future we would like it to be 64 i.e. do 64 eviction flushes together) from the list of initially `Cool` pages
-   `join!` the following page eviction futures:
    -   For each page Px we want to evict:
        -   Check if Px has been changed to `Hot`, and if so, return early
        -   Write-lock Px
        -   If Px is now either `Hot` or `Unloaded`, unlock and return early
        -   Write Px's buffer data out to disk via the local `io_uring` instance
        -   `await` write completion from the local `io_uring` instance
        -   Set Px to `Unloaded`
        -   Send Px's frame to the global channel of free frames
        -   Unlock Px
    
# Glossary
> If you are introducing new concepts or giving unintuitive names to components, write them down here.

- "Vectorized execution" is the name given to the concept of outputting batches of data. But since there is a `Vec`tor type in Rust, we'll likely be calling everything Batches instead of Vectors.