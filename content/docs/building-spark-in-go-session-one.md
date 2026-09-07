---
date: 2026-09-06T00:00:00-07:00
draft: false
title: 'Building Spark Core Execution Architecture in Go: Session One'
description: 'Exploring Spark execution architecture through RDD lineage, stage planning, and concurrent task execution in Go.'
categories:
  - 'Spark'
weight: 1
---

Spark is used daily in data engineering for large-scale distributed data processing. I’m building Spark’s core execution architecture in Go to explore the systems behind distributed computation and learn through implementation. The work will be divided into three sessions, each with a different focus.
Before starting the project, it helps to understand Spark’s core components and how they relate to one another. RDDs are Spark’s fundamental abstraction. We can think of an RDD as a metadata container carrying three main pieces of information:
1. Dataset computation functions: allow Spark to stream and process the dataset’s elements.
2. Partitions: describe how the dataset is divided.
3. Lineage: records parent RDDs to describe how the data is derived.
Note: Spark’s RDD abstraction also provides partition-computation behavior
With these components in mind, we can walk through Spark’s core workflow. When a user submits a Spark job through spark-submit, the cluster manager—Kubernetes in this example—launches a driver pod. The driver creates a SparkContext as the entry point and coordinates the job’s execution. Let’s look at how a submitted job becomes executable tasks.

{{< figure src="images/spark/decompositoinSketch.png" alt="Spark execution workflow decomposition sketch" >}}


1. Source RDDs and transformations construct the lineage.
2. DAGScheduler creates stages based on narrow and wide transformations.
3. DAGScheduler creates tasks for runnable stages, with one task per partition.
4. TaskScheduler assigns task attempts.
5. Executors run partition pipelines.
6. Results return to the driver.


Session one focuses on building an MVP Spark execution engine while exploring RDD lineage, lazy evaluation, narrow and shuffle dependencies, and partition-level tasks. Execution focuses on narrow transformations within a single process, without distributed execution.
I begin by defining an RDDNode Go struct containing the core metadata discussed above. Operator describes the operation and stores the function ID for transformations, Dependencies describes the lineage through parent RDDs, and NumPartitions specifies how many logical partitions the RDD has.
{{< figure src="images/RDDNode.png" alt="RDDNode Go struct" >}}
I also include a Partitioner property. Unlike NumPartitions, it describes a stronger property: which destination partition should contain a particular key when a shuffle occurs. For a hash partitioner, the configuration is used as follows:
partitionID := hash(key) % numPartitions
This guarantees that the same key always maps to the same partition.
{{< figure src="images/PartitionSpec.png" alt="PartitionSpec configuration" >}}
RDD nodes and their metadata are stored in RDDGraph, a driver-owned map. The graph deep-copies these nodes to prevent callers from accidentally changing the recorded lineage. Actual records are not loaded into the driver before execution. Recording operations and metadata without loading data or running the functions demonstrates Spark’s lazy evaluation.
An action such as Count or Collect triggers planning and execution. The planner starts from the action’s target and follows parent references in RDDGraph, working backward to discover the computation needed to produce the target. For RDDs with narrow dependencies, an output partition can be computed directly from its parent partition, allowing the operations to run within one task.

```text
RDD 0: TextFile(4 partitions)
  ↓ narrow
RDD 1: MapToPair
  ↓ shuffle
RDD 2: ReduceByKey(2 partitions)
  ↓ narrow
RDD 3: MapValues
  ↓
Collect()
```

```go
// Start from the action target.
ActionSpec{
    Kind:      ActionCollect,
    TargetRDD: 3,
}
```

When the planner encounters a shuffle dependency, computing one reduce partition requires records from multiple upstream partitions. It builds a separate shuffle-map stage, establishing a stage boundary. (Shuffle execution is not implemented in session one)

Working on stage generation reminded me of an interesting case I encountered at work. We had a relatively simple Spark job containing only narrow transformations that provided users with snapshots of dimension tables. The data volume was large, and we wanted to deliver the results as multiple reasonably sized files instead of one huge file, so we used coalesce to control the output partition count.
When reducing a larger partition count to 10 with coalesce(10), Spark groups existing partitions into 10 output partitions without a shuffle. This means the output stage has 10 tasks, with at most 10 running concurrently, depending on available executor slots. For a simple unpartitioned file write, this typically produces one data file per output partition, although file sizes may vary. In our case, reducing the partition count also limited the parallelism of the narrow pipeline and increased the runtime. It’s interesting to connect these real work experiences with the Spark execution architecture I’m building here.

Once the stages are planned, I generate one task per partition of each stage.
{{< figure src="images/GenerateTasks.png" alt="Task generation for stage partitions" >}}
The code follows this general structure:

```text
for each stage {
    for partitionID := 0; partitionID < stage.NumPartitions; partitionID++ {
        create task(stage, partitionID)
    }
}
```


Finally, I create execution APIs that connect the plan to actual work. Session one does not include distributed execution: the DAG scheduler, stage planning, task generation, and execution all run in one Go process.
I manage concurrency using goroutines to run tasks and two channels with different responsibilities. Go’s built-in support for concurrency is the primary reason I chose it for this project.
The scheduler launches one goroutine per task, as shown in scheduler/dag_scheduler.go.
Before executing runTask, each goroutine must acquire a slot by sending into the runner’s buffered permits channel. The channel’s capacity sets the concurrency limit. When it is full, additional goroutines wait. A deferred receive releases the slot when execution finishes, while context cancellation allows waiting tasks to exit. This logic lives in executor/local_runner.go.
Task goroutines report success or failure through a separate event channel. A single scheduler event-loop goroutine processes these events and updates job state, as shown in scheduler/event_loop.go. Tasks execute concurrently, while completion counts, results, and job state are updated one event at a time.
