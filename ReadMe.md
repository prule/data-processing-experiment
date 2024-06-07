Data Processing Experiment - Part 17
---
- Applying a real world example to improve the framework

---

> The code for this project is available in GitHub - I’m using a branch for each part and merging each part into the **[latest](https://github.com/prule/data-processing-experiment/tree/latest)** branch. See the ReadMe.md in each branch for the story.
>
> - [Github repository for this project](https://github.com/prule/data-processing-experiment/)
> - [Pull requests for each part](https://github.com/prule/data-processing-experiment/pulls?q=is%3Apr+is%3Aclosed)
> - [Branch for part-17](https://github.com/prule/data-processing-experiment/tree/part-17)

---

## Introduction



## Details

- count nulls - should be covered by EmptyCount
- shape - covered by row count and col count
- describe - covered by summary

I noticed 25 occurrences of these:
24/06/02 14:26:24 INFO FileScanRDD: Reading File path: file:///Users/paulrule/IdeaProjects/data-processing-experiment-2/data/part17/downloaded/addresses.csv, range: 0-7407373, partition values: [empty row]

Without persist in DataFrameBuilder.raw:
24/06/02 14:26:44 INFO Part17: Took 00:00:31.119

With persist in DataFrameBuilder.raw:
24/06/02 14:30:34 INFO Part17: Took 00:00:12.896

DataFrame.persist() The persist method is used to persist or cache a Spark DataFrame or Dataset in memory. This can improve performance for iterative operations or when the same data needs to be accessed multiple times.

The persist method takes an optional argument storageLevel that specifies the storage level for persisting the data. For example, persisted_df = df.persist(StorageLevel.MEMORY_AND_DISK_SER) would persist the data in memory and on disk in serialized format.

You should use persist() in Apache Spark when you need to cache or persist a Spark DataFrame or Dataset in memory or on disk for better performance in the following scenarios:
Iterative Operations: If you are performing multiple operations on the same DataFrame or Dataset, it can be beneficial to persist the data in memory or on disk to avoid recomputing it for each operation. This can significantly improve performance, especially for complex operations or large datasets.
Reusing Data: If you need to reuse the same DataFrame or Dataset multiple times in your application, persisting it can avoid recomputing or re-loading the data from the original source (e.g., file, database) each time.
Shared Data: If multiple tasks or operations need to access the same DataFrame or Dataset, persisting it can avoid redundant computations and improve overall performance.
Long-running or Exploratory Analysis: In long-running or exploratory data analysis tasks, where you might need to access the same data multiple times, persisting the data can significantly reduce the overall execution time.
Shuffle Operations: Operations that involve shuffling data across partitions, such as join, repartition, or groupBy, can benefit from persisting the input DataFrame or Dataset, as it can avoid recomputing the shuffle step multiple times.
However, it's important to note that persisting data in memory or on disk comes with a cost of increased memory or disk usage. Therefore, you should carefully evaluate the trade-off between performance gain and resource consumption when deciding to persist data. It's generally recommended to unpersist the data when it's no longer needed to free up resources.
Additionally, you should choose the appropriate storage level (StorageLevel) based on your use case and available resources. For example, MEMORY_AND_DISK can be used when the data doesn't fit entirely in memory, or DISK_ONLY can be used when you have limited memory but sufficient disk space.

