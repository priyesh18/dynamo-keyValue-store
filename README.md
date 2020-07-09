# Amazon Dynamo-style Replicated Key-Value Storage
1. Each android app instance(total 5) has an activity and a content provider.
2. The content provider instances implement the storage functionality using Java Sockets.
3. It supports insert, delete and query operations.
## Requirements/Goals
1. The main goal is to provide both availability and linearizability at the same time. In other words, your implementation should always perform read and write operations successfully even under failures. At the same time, a read operation should always return the most recent value.
2. Support insert/query/delete operations. Also, you need to support @ (all key in one device) and * (all keys in all devices) queries.
3. All failures are temporary; you can assume that a failed node will recover soon, i.e., it will not be permanently unavailable during a run.
4. When a node recovers, it should copy all the object writes it missed during the failure. This can be done by asking the right nodes and copy from them.
4. Your content provider should support concurrent read/write operations.
5. Your content provider should handle a failure happening at the same time with read/write operations.
4. Replication should be done exactly the same way as Dynamo does. In other words, a (key, value) pair should be replicated over three consecutive partitions, starting from the partition that the key belongs to.

## Implementation Details
1. Each Android Virtual Device is mapped to an `Avd` class that takes `port` as constructor parameter and maintains a `preference list` for replication.
1. My implementation uses chain replication which provides linearizability.
2. If you are interested in more details, please take a look at the following paper: http://www.cs.cornell.edu/home/rvr/papers/osdi04.pdf
3. Just as the original Dynamo, each request is used to detect a node failure.
4. Virtual nodes and Hinted Handoff is not implemented from the Dynamo paper.


Reference: [Dynamo Paper](http://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)  
Project is part of [CSE 586 - Distributed Systems by Professor Steve Ko](https://cse.buffalo.edu/~stevko/courses/cse486/spring20/index.html).