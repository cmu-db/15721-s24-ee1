---
marp: true
theme: default
#class: invert # Remove this line for light mode
paginate: true
---

# Eggstrain

Vectorized Push-Based inspired Execution Engine
Asynchronous Buffer Pool Manager

<br>

## **Authors: Connor, Sarvesh, Kyle**

---

# Original Proposed Goals

-   75%: First 7 operators working + integration with other components
-   100%: All operators listed above working
-   125%: TPC-H benchmark working

---

# Design Goals

-   Robustness
-   Modularity
-   Extensibility
-   Forward Compatibility

We made heavy use of `tokio` and `rayon` in our implementation.

---

# Refresher on Architecture

![bg right:60% 100%](./images/architecture.drawio.svg)

---

# Refresher on operators

-   `TableScan`
-   `Filter`
-   `Projection`
-   `HashAggregation`
-   `HashJoin` (`HashProbe` + `HashBuild`)
-   `OrderBy`
-   `TopN`

---

# Example Operator Workflow

![bg right:70% 80%](./images/hashjoin.svg)

---

# Progress Towards Goals

-   100%: All operators implemented, excluding `HashJoin`
-   125%: TPC-H benchmark working for Q1

---

# Execution Engine Benchmarks

Hardware:

-   M1 Pro, 8 cores, 16GB RAM

---

![bg 90%](./images/csvreader.png)

---

# Correctness Testing and Code Quality Assessment

We tested correctness by comparing our results to the results of the same queries run in DataFusion.

Our code quality is high with respect to documentation, integration tests, and code review.

However, we lack unit tests for each operator. We instead tested operators integrated inside of queries.

---

# Problem: In Memory?

We found that we needed to spill data to disk to handle large queries.

However, to take advantage of our asynchronous architecture, we needed to implement an **asynchronous buffer pool manager.**


---

# Recap: Buffer Pool Manager

A buffer pool manager manages synchronizing data between volatile memory and persistent storage.

*   In charge of bringing data from storage into memory in the form of pages
*   In charge of synchronizing reads and writes to the memory-local page data
*   In charge of writing data back out to disk so it is synchronized

---

# Traditional Buffer Pool Manager

![bg right:50% 100%](images/traditional_bpm.png)

Traditional BPMs will use a global hash table that maps page IDs to memory frames.

*   Source: _LeanStore: In-Memory Data Management Beyond Main Memory (2018)_

---

# Recap: Blocking I/O

Additionally, traditional buffer pool managers will use blocking reads and writes to send data between memory and persistent storage.

Blocking I/O is heavily reliant on the Operating System.

> The DBMS can almost always manage memory better than the OS

*   Source: 15-445 Lecture 6 on Buffer Pools

---

# Recap: I/O System Calls

What happens when we issue a `pread()` or `pwrite()` call?

*   We stop what we're doing
*   We transfer control to the kernel
*   _We are blocked waiting for the kernel to finish and transfer control back_
    *   _A read from disk is *probably* scheduled somewhere_
    *   _Something gets copied into the kernel_
    *   _The kernel copies that something into userspace_
*   We come back and resume execution

---

# Blocking I/O for Buffer Pool Managers

Blocking I/O is fine for most situations, but might be a bottleneck for a DBMS's Buffer Pool Manager.

-   Typically optimizations are implemented to offset the cost of blocking:
    -   Pre-fetching
    -   Scan-sharing
    -   Background writing
    -   `O_DIRECT`

---

# Non-blocking I/O

What if we could do I/O _without_ blocking? There exist a few ways to do this:

-   `libaio`
-   `io_uring`
-   SPDK
-   All of these allow for _asynchronous I/O_

---

# `io_uring`

![bg right:50% 90%](images/linux_io.png)

This Buffer Pool Manager is going to be built with asynchronous I/O using `io_uring`.

*   Source: _What Modern NVMe Storage Can Do, And How To Exploit It... (2023)_

---

# Asynchronous I/O

Asynchronous I/O really only works when the programs running on top of it implement _cooperative multitasking_.

*   Normally, the kernel gets to decide what thread gets to run
*   Cooperative multitasking allows the program to decide who gets to run
*   Context switching between tasks is a _much more_ lightweight maneuver
*   If one task is waiting for I/O, we can cheaply switch to a different task!

---

# Eggstrain

The key thing here is that our Execution Engine `eggstrain` fully embraces asynchronous execution.

*   Rust has first-class support for asynchronous programs
*   Using `async` libraries is almost as simple as plug-and-play
*   The `tokio` crate is an easy runtime to get set up
*   We can easily create a buffer pool manager in the form of a Rust library crate

---

# Goals

The goal of this system is to _fully exploit parallelism_.

*   NVMe drives have gotten really, really fast
*   Blocking I/O simply cannot match the full throughput of an NVMe drive
*   They are _completely_ bottle-necked by today's software
*   If we can fully exploit parallelism in software _and_ hardware, we can get close to matching the speed of in-memory systems, while using persistent storage

---

![bg 60%](images/modern_storage.png)

---

# Proposed Design

The next slide has a proposed design for a fully asynchronous buffer pool manager. The full (somewhat incomplete) writeup can be found [here](https://github.com/Connortsui20/async-bpm).

*   Heavily inspired by LeanStore
    *   Eliminates the global page table and uses tagged pointers to data
*   Even more inspired by this paper:
    *   _What Modern NVMe Storage Can Do, And How To Exploit It: High-Performance I/O for High-Performance Storage Engines (2023)_
        *   Gabriel Haas and Viktor Leis
*   The goal is to _eliminate as many sources of global contention as possible_

---

![bg 90%](images/bpm_design.png)

---

# BPM Benchmarks

Hardware:

* Cray/Appro GB512X - 32 Threads Xeon E5-2670 @ 2.60GHz, 64 GiB DDR3 RAM, 1x 240GB SSD, Gigabit Ethernet, QLogic QDR Infiniband

* We will benchmark against RocksDB as a buffer pool manager.

---

![bg 90%](./images/zip1.1dist.png)

---

![bg 90%](./images/20w80r.png)

<!--
zipfian distribution, alpha = 1.01 -->

---

![bg 90%](./images/80w20r.png)

---

![bg 90%](./images/uniform80w20r.png)

---

![bg 90%](./images/uniform20w80r.png)

---

![bg 90%](./images/uniform5050.png)

<!-- zipfian distribution, alpha = 1.01 -->

<!-- ---

![bg 90%](./images/zip1.1.png)

zipfian distribution, alpha = 1.1 -->

<!-- ---

![bg 90%](./images/zip1.2.png)
zipfian distribution, alpha = 1.2 -->

---

# Future Work

-   Asynchronous BPM ergonomics and API
-   Proper `io_uring` polling and batch evictions
-   Shared user/kernel buffers and file descriptors (avoiding `memcpy`)
-   Multiple NVMe SSD support (Software-implemented RAID 0)
-   Optimistic hybrid latches
