---
date: 2026-10-08T00:00:00-07:00
draft: false
title: 'Sparkcore-Go: Shuffle Execution and Worker Failure Recovery'
description: 'Implementing shuffle execution with shared filesystem storage, worker lifecycle management, and bounded shuffle-stage recovery in Go.'
categories:
  - 'Spark'
weight: 4
---

Github Link: [Sparkcore-Go](https://github.com/Wendyddw/sparkcore-go)

Now comes the last part of this Spark-core project: implementing shuffle execution and worker failure recovery, built on top of Session 2’s distributed executor and HTTP coordinator. Shuffle is often one of the most expensive and resource-intensive operations in Spark. It’s interesting to dig into the actual process of redistributing data across partitions and executor nodes in a “cluster.” Along with shuffle execution, we also bring in worker lifecycle management and bounded shuffle-stage recovery to demonstrate fault tolerance through recomputation.

### Shuffle workflow

First, let’s quickly go over how a conventional Spark shuffle works for an operation like ReduceByKey:

1. Map tasks partition their output and write shuffle data to executor-local disk.
2. The driver tracks successful map-output locations through its map-output tracking machinery.
3. Executors running reduce tasks obtain those locations and fetch the required blocks, reading local data locally and remote data over the network.
4. Each reduce task aggregates the records for its partition. The intermediate shuffle records do not pass through the driver.

This involves disk I/O, serialization, and network transfer, which contribute to shuffle’s cost. Spark shuffle documentation

To mimic and simplify this process in our project, from mappers storing intermediate data to reducers reading shuffle inputs, we use a shared filesystem that workers can access directly. This lets us focus on shuffle execution without also building shuffle-serving endpoints and tracking which worker serves each output. Workers report output metadata back to the coordinator, and the schedulers track which outputs are accepted.

{{< figure src="images/spark/shuffle-workflow.svg" link="/images/spark/shuffle-workflow.svg" alt="Map A and Map B write reducer buckets to a shared filesystem; reduce task 0 reads bucket 0 and reduce task 1 reads bucket 1" >}}

Each mapper hashes keys into reducer buckets and publishes its files. After all map outputs are accepted, each reducer reads its bucket from every mapper and aggregates values by key.

Let’s walk through the shuffle workflow with filesystem integration using some pseudocode.

1\. Map tasks write and publish shuffle files

Each map task processes its input partition and writes key-value records into a separate directory for its execution attempt.

```go
writer := store.Begin(ctx, attemptIdentity, numMaps, numReducers)

for record := range mapOutput {
    writer.Write(record)
}

metadata := writer.Publish()
return TaskOutput{ShuffleOutput: metadata}
```

Inside the filesystem writer, each record’s key determines its bucket:

```go
bucket := FNV1a64(record.Key) % numReducers
encodeAndAppend(bucketFile[bucket], record)
```

The reducer count is configured before execution. The writer assigns records to buckets during execution. Optional map-side combining can aggregate repeated keys before writing.

Publish atomically makes the completed map output available. Each attempt uses a distinct path, so retries do not overwrite earlier output.

Code: [executor/shuffle_map.go](https://github.com/Wendyddw/sparkcore-go/blob/main/executor/shuffle_map.go), [shuffle/writer.go](https://github.com/Wendyddw/sparkcore-go/blob/main/shuffle/writer.go), [shuffle/partitioner.go](https://github.com/Wendyddw/sparkcore-go/blob/main/shuffle/partitioner.go).

2\. Workers report published output metadata

```go
output := executor.Run(assignment)

coordinator.ReportSuccess(
    assignment.Identity,
    output,
)
```

The worker sends metadata identifying the published output, including its attempt identity, bucket sizes, and checksums. Shuffle records remain on the shared filesystem; they do not pass through the coordinator.

Publication and acceptance are separate: a file can exist without becoming an accepted input for reducers.

Code: [worker/task.go](https://github.com/Wendyddw/sparkcore-go/blob/main/worker/task.go), [coordinator/task_report.go](https://github.com/Wendyddw/sparkcore-go/blob/main/coordinator/task_report.go).

3\. Schedulers accept map outputs and enforce the stage barrier

The FIFO task scheduler validates reports against assigned attempts before notifying the DAG scheduler. The DAG scheduler tracks accepted outputs for the current stage attempt.

```go
onMapSuccess(report):
    if report is stale or already accepted:
        return

    validate(report.output)
    acceptedOutputs[report.mapPartition] = report.output

    if every map partition has an accepted output:
        inputs := snapshot(acceptedOutputs)
        submitReduceTaskSet(inputs)
```

Reducers start only after all map outputs are accepted. Their input snapshot identifies the exact outputs to read—they do not scan directories to discover files.

Code: [scheduler/fifo_task_scheduler.go](https://github.com/Wendyddw/sparkcore-go/blob/main/scheduler/fifo_task_scheduler.go), [scheduler/dag_scheduler.go](https://github.com/Wendyddw/sparkcore-go/blob/main/scheduler/dag_scheduler.go), [scheduler/dag_stage_execution.go](https://github.com/Wendyddw/sparkcore-go/blob/main/scheduler/dag_stage_execution.go).

4\. Each reducer reads its bucket from every accepted map output

```go
// Shuffle-read iterator:
for mapOutput := range inputs.Outputs {
    reader := store.OpenBucket(ctx, mapOutput, reducePartitionID)

    for record := range reader {
        yield record
    }

    reader.Close()
}
```

Reduce task 0 reads bucket 0 from every accepted map output. Reduce task 1 reads bucket 1, and so on. The reader validates bucket contents as they are consumed.

Code: [executor/shuffle_read.go](https://github.com/Wendyddw/sparkcore-go/blob/main/executor/shuffle_read.go).

5\. ReduceByKey aggregates records by key

The shuffle-read iterator feeds a separate aggregation step:

```text
values := map[Key]Value{}

for record := range shuffleReadIterator {
    if record.Key exists in values {
        values[record.Key] = reduce(values[record.Key], record.Value)
    } else {
        values[record.Key] = record.Value
    }
}

return recordsSortedByKey(values)
```

For a sum operation, reduce(previous, current) adds the values. Different keys can share a bucket, so the reducer still groups records by their actual keys.

Code: [executor/reduce.go](https://github.com/Wendyddw/sparkcore-go/blob/main/executor/reduce.go).

This wraps up the shuffle implementation. Going through these steps, we can see why redistributing data is expensive: it brings together several potential system bottlenecks, including network transfers between mapper and reducer nodes, intermediate disk reads and writes, and data serialization and deserialization. (Our shared-filesystem setup simplifies the transfer mechanism, so it does not reproduce all of these costs in the same way.)

Out-of-memory errors can also happen during shuffle execution. On the map side, sorting and aggregation can put pressure on memory. Spark can spill intermediate structures to disk, so the entire map output does not have to fit in memory at once. On the reducer side, uneven key distribution can create skewed partitions. For example, when many records share a null key—and leave one task with a disproportionate amount of work.

Our implementation keeps combining and reduction in memory and does not implement spilling. This exercise helps me strengthen my understanding of the shuffle workflow and avoid some common mistakes in the future.

### Worker lifecycle handling

Along with shuffle execution, we also need to handle workers that stop reporting. Workers register with the coordinator and send periodic heartbeats. A background monitor in the coordinator triggers the scheduler’s expiry check; if a worker has been silent beyond the timeout, the scheduler marks it lost and requeues its unfinished tasks while the retry budget allows. Fresh attempt IDs keep late reports from changing accepted results. Since we use shared storage, accepted shuffle outputs can survive worker termination. If an accepted output later becomes missing or corrupt, the DAG scheduler uses a separate stage-recovery budget to rerun the full map stage and then the result stage. This gives us two recovery paths: retry unfinished work after worker loss, and regenerate shuffle data when accepted input becomes unavailable.

Code: [coordinator/worker_monitor.go](https://github.com/Wendyddw/sparkcore-go/blob/main/coordinator/worker_monitor.go), [scheduler/worker_expiry.go](https://github.com/Wendyddw/sparkcore-go/blob/main/scheduler/worker_expiry.go), [scheduler/dag_recovery.go](https://github.com/Wendyddw/sparkcore-go/blob/main/scheduler/dag_recovery.go).
