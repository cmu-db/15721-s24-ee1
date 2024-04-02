---
marp: true
theme: default
class: invert # Remove this line for light mode
paginate: true
---


# Asynchronous Buffer Pool Manager

## **Authors: Connor, Kyle, Sarvesh**


---


# Recap: Buffer Pool Manager

A buffer pool manager manages synchronizing data between volatile memory and persistent storage.

* In charge of bringing data from storage into memory in the form of pages
* In charge of synchronizing reads and writes to the memory-local page data
* In charge of writing data back out to disk so it is synchronized


---


# Traditional Buffer Pool Manager

![bg right:50% 100%](images/traditional_bpm.png)

Traditional BPMs will use a global hash table that maps page IDs to memory frames.

* Source: _LeanStore: In-Memory Data Management Beyond Main Memory (2018)_


---


# Recap: Blocking I/O

Additionally, traditional buffer pool managers will use blocking reads and writes to send data between memory and persistent storage.

Blocking I/O is heavily reliant on the Operating System.

> The DBMS can almost always manage memory better than the OS

* Source: 15-445 Lecture 6 on Buffer Pools


---


# Recap: I/O System Calls

What happens when we issue a `pread()` or `pwrite()` call?

* We stop what we're doing
* We transfer control to the kernel
* _We are blocked waiting for the kernel to finish and transfer control back_
    * _A read from disk is *probably* scheduled somewhere_
    * _Something gets copied into the kernel_
    * _The kernel copies that something into userspace_
* We come back and resume execution


---


# Blocking I/O for Buffer Pool Managers

Blocking I/O is fine for most situations, but might be a bottleneck for a DBMS's Buffer Pool Manager.

* Typically optimizations are implemented to offset the cost of blocking:
    * Pre-fetching
    * Scan-sharing
    * Background writing
    * `O_DIRECT`


---


# Non-blocking I/O

What if we could do I/O _without_ blocking? There exist a few ways to do this:

* `libaio`
* `io_uring`
* SPDK
* All of these allow for _asynchronous I/O_


---


# `io_uring`

![bg right:50% 90%](images/linux_io.png)

This Buffer Pool Manager is going to be built with asynchronous I/O using `io_uring`.

* Source: _What Modern NVMe Storage Can Do, And How To Exploit It... (2022)_


---


# Asynchronous I/O

Asynchronous I/O really only works when the programs running on top of it implement _cooperative multitasking_.

* At a high level, the kernel gets to decide what thread gets to run
* Cooperative multitasking allows the program to decide


---





