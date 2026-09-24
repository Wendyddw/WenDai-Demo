---
date: 2026-09-23T22:36:38-07:00
draft: false
title: 'Mini Feature Store: Structuring the Offline Store'
description: 'A data engineering perspective on feature stores and offline batch jobs with Scala and Spark.'
categories:
  - 'Spark'
weight: 3
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

This wraps up the Spark job setup. In this repo, `BackfillPipeline` aggregates raw events into daily user features, including rolling event counts and days since the last event within the lookback window, then writes them to Iceberg partitioned by day. `PointInTimeJoinPipeline` joins labels with the latest feature snapshot on or before each label’s date and writes training data to Parquet.

Time comparisons use calendar dates, so future-day snapshots are excluded, but same-day leakage remains possible.

That’s it for the offline setup for now. In the next part, I’ll walk through the online store and how we serve these features for real-time predictions. To be continued!
