---
date: 2026-09-14T22:48:56-07:00
draft: false
title: 'Building a Spark-Style Execution Engine in Go: Distributed Task Execution'
description: 'Extending the Go execution engine with FIFO scheduling, independent worker processes, and HTTP communication for distributed task execution.'
categories:
  - 'Spark'
weight: 2
---

In [session one]({{< ref "building-spark-in-go-session-one.md" >}}), we successfully implemented RDD lineage building, stage planning, and partition-level tasks in a single Go process. Goroutines provide concurrency, and channels coordinate that concurrency at the task execution level. But all goroutines share the same memory, function registry, filesystem access, and process lifecycle. All components share a process, so an unrecovered panic in one goroutine can bring down the entire application.

In this session, we replace the single process with distributed execution to establish process and network boundaries. We introduce a physical scheduler and multiple independent worker processes that communicate with the coordinator over HTTP, like deploying microservices on AWS.

{{< figure src="images/spark/distributed_executions.png" alt="Moving from a single Go process to a driver and independent worker processes communicating over HTTP" >}}

The key components of this session are:

1. A scheduler that handles worker registration, heartbeats, task assignments, and result collection.
2. A corresponding worker workflow for communicating available resources, processing tasks, and reporting results.

Let’s start with the scheduler and scheduling policies. Here, we mimic [Spark’s FIFO scheduling policies](https://spark.apache.org/docs/latest/job-scheduling.html) with similar capacity-matching rules, but simplify some internal mechanisms and use a queue-based scheduling model. We set the following three rules:

1\. Select the oldest task set with pending tasks.

Spark schedules by active stages and priority pools rather than strictly picking the “oldest” task set globally.
The scheduler first appends each accepted task set to `s.queue` ([scheduler/fifo_task_scheduler.go, line 136](https://github.com/Wendyddw/sparkcore-go/blob/main/scheduler/fifo_task_scheduler.go#L136)), then scans that queue in insertion order ([line 239](https://github.com/Wendyddw/sparkcore-go/blob/main/scheduler/fifo_task_scheduler.go#L239)).

```go
// Append task sets in admission order.
s.queue = append(s.queue, set)

// Visit them in the same order.
for _, set := range s.queue {
    if set.terminal {
        continue
    }
    if err := set.ctx.Err(); err != nil {
        s.finish(set, err)
        continue
    }
    // Assign pending tasks.
}
```

2\. Select its lowest pending partition.

The scheduler prepares task sets and sorts their tasks by ascending `PartitionID` ([scheduler/fifo_task_scheduler.go, line 188](https://github.com/Wendyddw/sparkcore-go/blob/main/scheduler/fifo_task_scheduler.go#L188)). It then selects `set.tasks[set.pending]` and advances the cursor ([line 247](https://github.com/Wendyddw/sparkcore-go/blob/main/scheduler/fifo_task_scheduler.go#L247)).

```go
// Sort once when preparing the task set.
sort.Slice(set.tasks, func(i, j int) bool {
    return set.tasks[i].task.PartitionID < set.tasks[j].task.PartitionID
})

// Take the next pending partition during assignment.
task := set.tasks[set.pending]
set.pending++
```

3\. Assign work up to the offering worker’s available capacity.

The scheduler starts with the worker’s reported free slots. Some slots may already be reserved for tasks whose assignments are still on the way, so the worker hasn’t included them in its heartbeat yet. It subtracts those reservations to avoid assigning more work than the worker can handle and checks that the result stays within the worker’s total unreserved capacity. Then it assigns tasks while slots remain, reserving one slot and reducing `available` for each assignment (`scheduler/fifo_task_scheduler.go`, [line 232](https://github.com/Wendyddw/sparkcore-go/blob/main/scheduler/fifo_task_scheduler.go#L232)).

```go
available := free
for attemptID := range worker.reserved {
    if !seen[attemptID] {
        available--
    }
}
available = min(available, worker.TotalSlots-len(worker.reserved))

for available > 0 && set.pending < len(set.tasks) {
    // Select task and create assignment...
    worker.reserved[identity.ID] = struct{}{}
    assignments = append(assignments, assignment)
    available--
}
```

The worker, meanwhile, has its own essential workflow, from reporting its status to claiming tasks from the scheduler. Let’s take a high-level look at the worker’s lifecycle (`worker/runtime.go`, line 92).

{{< figure src="images/spark/worker.png" alt="Worker lifecycle: register, send heartbeats, receive assignments, execute, report results, and free slots" >}}

On startup, the worker sends its ID and total slot capacity to the coordinator as a one-time initial registration. It then sends a heartbeat immediately and periodically afterward, containing free slots and active attempt IDs to report its status and request work.

Upon receiving tasks, the worker validates the assignments, marks them active locally, and launches each in a goroutine. `LocalRunner.RunTask` executes the partition pipeline. Once execution completes, the worker sends either the task’s output or its failure to the coordinator interface, which we’ll discuss in the next section. The coordinator forwards it to the scheduler.

After the report is acknowledged, the worker releases local capacity and repeats the process. The worker only shuts down when canceled or when communication fails.

Now that we’ve reviewed the core workflows of the scheduler and worker, the remaining question is: how do these two components communicate with each other?
Here, we introduce the coordinator interface, which handles network communication between the scheduler and worker processes. For this project, our immediate messages are registration, heartbeats, assignments, and results. They fit the request/response model well.

We choose to use a RESTful API with HTTP and JSON for rapid prototyping, allowing us to concentrate on the more complicated scheduling behavior: reservations, stale attempts, cancellation, and worker failure. For production, we would use gRPC with Protocol Buffers as the payload for more efficient and type-safe internal service-to-service communication.

One more thing to mention is that the coordinator shares the same Go process as the scheduler, but deliberately handles HTTP endpoints, JSON decoding, request validation, and error responses to avoid mixing network handling with scheduler placement logic.

Here is the list of endpoints that handle communication between workers and the scheduler:

| Endpoint | Worker sends | Scheduler call | Response |
| --- | --- | --- | --- |
| /v1/workers/register | Worker ID and total slots | RegisterWorker | Confirmed identity and capacity |
| /v1/workers/heartbeat | Free slots and active attempt IDs | OfferResources | New task assignments, or an empty list |
| /v1/tasks/success | Attempt identity and partition output | ReportSuccess | Acknowledgment |
| /v1/tasks/failure | Attempt identity and error | ReportFailure | Acknowledgment |

This wraps up our distributed task execution implementation. Here’s a screenshot showing how a coordinator interacts with two worker processes, each offering two slots, through initial registration, resource offers, task assignments, and result reporting.

{{< figure src="images/spark/log_screenshot.png" alt="Logs showing coordinator and worker registration, resource offers, task assignments, and result reporting" >}}

I used manual commands to spin up the services here. A future improvement is to package the coordinator and workers with Docker Compose, replacing separate terminal commands with a reproducible setup that includes service networking, shared input files, and combined logs.
