---
date: 2026-09-23T22:36:38-07:00
draft: false
title: 'Mini Feature Store: Structuring the Offline Store'
description: 'A data engineering perspective on feature stores and offline batch jobs with Scala and Spark.'
categories:
  - 'Feature Engineering'
weight: 1
---

Github Link: [Mini Feature Store](https://github.com/Wendyddw/mini-feature-store)

Today I’m writing about a project I did a year ago called mini feature store. Feature engineering has grown alongside machine learning. In short, feature engineering covers transforming raw data into variables (features) for machine learning. A feature store helps store, catalog, and serve those features.

I want to start with the project setup and discuss it from a data engineering perspective to help developers and data engineers understand feature engineering through familiar concepts and setups. Essentially, the data processing side of feature engineering is data engineering tailored for machine learning. They share lots of things in common, like using Spark for data transformations and some base infrastructure (e.g., Kubernetes for cluster management, S3 as storage, and Iceberg or another open table format).

Aside from the similarities, there are two key requirements that feature stores emphasize. ML data may also include vectors or tensors, although many features are ordinary numeric or categorical values.

1. The storage is often **dual tier**: a traditional lakehouse for historical offline features plus a low-latency online store using databases like Redis or DynamoDB for online serving.

2. Data processing with time handling: offline feature processing must account for **event time and point-in-time correctness** to prevent data leakage. These concerns also exist in general data engineering, but they are especially important when building training data.

{{< figure src="images/feature-store/workflow.svg" link="/images/feature-store/workflow.svg" alt="Traditional data engineering workflow compared with feature engineering, showing offline training and online inference paths" >}}

### Structuring the Offline Feature Store

An offline feature store retains months or years of feature history for model training and backtesting. The processing jobs around it handle heavy transformations in batches. Let’s first dive into how I structure the offline feature jobs as a Scala Spark application, which carries over lots of learning from data engineering.

First, we **pick Scala over Python with PySpark**. I’ve worked with both in production settings. Scala is a statically typed language, and two major benefits I value are:

#### 1. Type safety at compile time

We use case classes to make domain models and configuration explicit. With Datasets typed using case classes, Scala helps catch type errors in constructors, field access, and typed transformations at compile time. This can save development time by catching mistakes before running a job.

Some quick examples from this project: under `types`, I define `EventRaw`, `Label`, `FeaturesDaily`, `TrainingData`, and pipeline configuration classes. These help catch incorrect field names and value types at compile time. For example, accessing `feature.event_count_7dd` on a `FeaturesDaily` object fails to compile because the field doesn’t exist. Its `event_count_7d` field is an `Option[Long]`, so the compiler also prevents us from treating it directly as a `Long` without handling the optional value.

The pipelines convert DataFrames into typed Datasets at selected points with `.as[FeaturesDaily]`, which enables compiler-checked field access in typed operations. However, the conversion’s schema compatibility and string-based expressions like `col("...")` are still checked at runtime. Local Spark tests help catch those errors before deployment.

```scala
case class FeaturesDaily(
  user_id: String,
  day: Date,
  event_count_7d: Option[Long],
  event_count_30d: Option[Long],
  last_event_days_ago: Option[Int],
  event_type_counts: Option[String]
)
val recentFeatures = platform.fetcher
  .readIcebergTable(spark, config.featuresTable)
  .filter(col("day") >= cutoffDate)
  .orderBy(col("day").desc)
  .as[FeaturesDaily]
```

#### 2. Support for functional programming

I have to admit I didn’t appreciate functional programming until I saw thousands of PySpark jobs running in production, consisting of hundreds of lines of nested SQL queries with no test setup at all. Testing often gets less attention in data engineering, especially when jobs are written in ways that make them difficult to test. Engineers end up busy firefighting production incidents caused by one-line changes.

Functional programming makes transformations and values easier to follow through immutable val bindings, transformation chains, `Option.map/getOrElse`, and pattern matching. **Separating pure functions from side effects** also makes code easier to test by reducing hidden state.

```scala
val readerWithSchema = schema match {
  case Some(s) => reader.schema(s)
  case None    => reader
}
```

With these two core benefits in mind, let’s quickly walk through how to set up the offline batch jobs from scratch.

#### 1. Use sbt as the build tool

Build tools handle code compilation, dependency management, and automated tests. Some common commands:

* `sbt assembly` packages the application into a runnable JAR using the sbt-assembly plugin.
* `sbt test` runs tests.
* `sbt "runMain com.example.featurestore.App ..."` runs the application locally.

#### 2. Use `App.scala` as the entry point

This file **separates orchestration from pipeline logic**. The App object’s main method parses arguments, creates the platform, dispatches to backfill, point-in-time join, or online sync, and stops Spark in a finally block.

#### 3. Pipeline classes contain the core business transformations

The `spark/src/main/scala/com/example/featurestore/pipelines` directory contains the business logic for feature aggregation, point-in-time joins, and online synchronization. Each pipeline currently combines reading inputs, transforming data, and writing outputs. A useful next step is to extract transformations into separate functions or files to make individual rules easier to reuse and unit test.

#### 4. A shared platform layer

`SparkPlatformTrait.scala` bundles the Spark session with reader and writer interfaces. `PlatformProvider` centralizes session creation and configuration.

The traits under platform define **shared contracts for infrastructure used in both production and tests**, with different implementations under the hood. Tests can replace storage with in-memory key-value maps while using the same method contracts.

| Dependency | Production | Tests |
| --- | --- | --- |
| Execution engine | Spark session | Real local Spark session |
| Reads | `ProdFetcher` | `TestFetcher` |
| Writes | `ProdWriter` | `TestWriter` |
| Pipeline logic | Same pipeline classes | Same pipeline classes |

#### 5. Test at two levels under `spark/src/test`

Existing component tests run complete pipelines on local Spark with in-memory `TestFetcher` and `TestWriter`. A next step is to add focused unit tests for extracted transformations and edge cases.

### Building Historical Features and Training Data

With the Spark Scala batch job structure in place, let’s dive into `BackfillPipeline` and `PointInTimeJoinPipeline`. These batch jobs build historical feature snapshots, then use that history to produce training examples with features matched to the prediction date. This provides date-level filtering, though full point-in-time correctness needs more precise time handling. Below is a workflow showing how we transform raw event data into training data.

{{< figure src="images/feature-store/training-workflow.svg" link="/images/feature-store/training-workflow.svg" alt="Raw Parquet events pass through BackfillPipeline into daily Iceberg feature history, then PointInTimeJoinPipeline joins labels and prediction timestamps to produce Parquet training data" >}}

Let’s walk through the jobs with sample data for a more straightforward view. We start with the following raw user events:

| user_id | event_type | ts |
| --- | --- | --- |
| user1 | click | Jan 1, 10:00 |
| user1 | purchase | Jan 3, 14:00 |
| user1 | click | Jan 5, 09:00 |

`BackfillPipeline` aggregates raw events into daily user features, including rolling event counts and days since the last event within the lookback window, then writes them to Iceberg partitioned by day. Each row describes the user’s activity through that calendar day.

| user_id | day | event_count_7d | event_count_30d | last_event_days_ago | event_type_counts |
| --- | --- | --- | --- | --- | --- |
| user1 | Jan 1 | 1 | 1 | 0 | "1" |
| user1 | Jan 2 | 1 | 1 | 1 | "1" |
| user1 | Jan 3 | 2 | 2 | 0 | "2" |
| user1 | Jan 4 | 2 | 2 | 1 | "2" |
| user1 | Jan 5 | 3 | 3 | 0 | "2" |

In this implementation, `event_type_counts` is the number of distinct event types, stored as a string.

`PointInTimeJoinPipeline` joins labels with the latest feature snapshot on or before each label’s date. Labels represent the target outcomes for the examples we want to train on. For illustration, the prediction task could be “Will this user purchase within the next 24 hours?” A label of `1.0` means yes, and `0.0` means no.

| user_id | as_of_ts | label |
| --- | --- | --- |
| user1 | Jan 2, 18:00 | 1.0 |
| user1 | Jan 4, 18:00 | 0.0 |

To produce the training data, `PointInTimeJoinPipeline` selects the latest snapshot on or before each label’s date and writes the joined rows to Parquet. Here, `as_of_ts` identifies the prediction time, while `day` identifies the selected feature snapshot. The model learns from the feature columns to predict `label`. This simple example has no same-day future events included because neither selected day has events after the prediction time.

| user_id | label | as_of_ts | day | event_count_7d | event_count_30d | last_event_days_ago | event_type_counts |
| --- | --- | --- | --- | --- | --- | --- | --- |
| user1 | 1.0 | Jan 2, 18:00 | Jan 2 | 1 | 1 | 1 | "1" |
| user1 | 0.0 | Jan 4, 18:00 | Jan 4 | 2 | 2 | 1 | "2" |

However, the current date-based join would also include later events on those days if they existed, so same-day leakage remains possible. This example also doesn’t establish whether the snapshots were actually available at prediction time.

Coming back to real-world use cases, the key to avoiding leakage is defining **when a feature’s window ends and when its value becomes available**. A common pattern is a timestamp-based **as-of join**: select the latest eligible feature value at or before the prediction time. Both [Feast](https://docs.feast.dev/getting-started/concepts/point-in-time-joins) and [Databricks](https://docs.databricks.com/aws/en/machine-learning/feature-store/time-series) support timestamp-based historical lookups; availability handling also needs to be considered in the pipeline design.

For our case, a snapshot with `day = Jan 3` includes events throughout that whole day. We can update the feature table to give snapshots explicit timestamps: keep `day` for partitioning, **add `window_end_ts` for the exclusive event cutoff**, and **add `available_at` for when that feature version became usable**. The join would then select the latest eligible snapshot whose window had ended and whose value was available by `as_of_ts`.

That’s it for the offline feature store for now. In the next part, I’ll walk through the online store and how we serve these features for real-time predictions. To be continued!
