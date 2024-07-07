## Overview

Data processing is on my radar at the moment, and taking a leaf from [John Cricket](https://www.linkedin.com/in/johncrickett/) what better way to get some "practice and experience" in Kotlin, Spark, Data Processing, Design, and Documentation than to build something...

So this is
- a coding challenge
- a design challenge
- a documentation/writing challenge

Read about it here https://paulr70.substack.com/p/data-processing-experiment-part-0

## The process

I'm going to put the code in a single git repository - in a real project I'd probably use multiple repos but to keep it self contained and simple I'll use one project with many subprojects - and I'll be using Kotlin, Gradle, Git, Intellij as the tools of choice. 

- https://github.com/prule/data-processing-experiment

I'm going to split the work into "parts" - simple progressions, each one building on the last, and I'll do these in git as separate branches. This way we'll be able to see how things progress over time as well as seeing the end result.

* [Part 0](https://paulr70.substack.com/p/data-processing-experiment-part-0)
* [Part 1](https://paulr70.substack.com/p/data-processing-experiment-part-1) - Setting up the project (a git repository with a gradle project)
* [Part 2](https://paulr70.substack.com/p/data-processing-experiment-part-2) - Some basic spark sql code, so we see how Spark SQL works and what's involved
  * At this point I'll need some data to work with so I'll create something super simple
* [Part 3](https://paulr70.substack.com/p/data-processing-experiment-part-3) - The code in part 2 established some patterns, but it's not flexible or reusable, so I'll refactor it
* [Part 4](https://paulr70.substack.com/p/data-processing-experiment-part-4) - Now I've got more generic code there's lots more we can do - I'll start by adding some basic form of validation
* [Part 5](https://paulr70.substack.com/p/data-processing-experiment-part-5) - After some basic validation I need some statistics about the data
* [Part 6](https://paulr70.substack.com/p/data-processing-experiment-part-6) - In this part, I add the capability to deduplicate, specify an alias for a column, and specify a delimiter for the CSV. Also added a duplicate statistic so duplicates can be quantified. Then it's time to load and process multiple tables - so I need some more sample data and to update the application to iterate over each of the tables, processing them and generating statistics.
* [Part 7](https://paulr70.substack.com/p/data-processing-experiment-part-7) - In this part, I look at the current state of things and ponder what the future may hold for this experiment. Then I add support for multiple column names (for when raw data arrives with inconsistently named columns), store the loaded data frames in a context, which will make them available to transformers - starting with the capability to Union, and setting the scene for part 8 which will introduce pipelines for transforms.
* [Part 8](https://paulr70.substack.com/p/data-processing-experiment-part-8) - It's time to add the capability to define a pipeline of tasks - so I can union tables, add literal columns, join tables and output tables to files. Polymorphic serialization was useful here!
* [Part 9](https://paulr70.substack.com/p/data-processing-experiment-part-9) - I add capabilities to apply some simple data cleaning tasks.
* [Part 10](https://paulr70.substack.com/p/data-processing-experiment-part-10) - I refactor to clean up and use more polymorphic serialization to simplify and reduce code.
* [Part 11](https://paulr70.substack.com/p/data-processing-experiment-part-11) - The one where I try out DataBricks Community Edition to do something similar to the Kotlin codebase.
* [Part 12](https://paulr70.substack.com/p/data-processing-experiment-part-12) - The one where I try out Google Colab notebooks to do something similar to the Kotlin codebase.
* [Part 13](https://paulr70.substack.com/p/data-processing-experiment-part-13) - The one where I try out using Pandas to manipulate data
* [Part 14](https://paulr70.substack.com/p/databricks-with-spark-partitioning) - Databricks with spark partitioning
* [Part 15](https://paulr70.substack.com/p/sampling-data-with-spark) - Sampling data with Spark
* [Part 16](https://paulr70.substack.com/p/stratified-data-sampling-with-spark) - Stratified data sampling with Spark
* [Part 17](https://paulr70.substack.com/p/data-processing-experiment-part-17) - Applying the framework to real world examples - [Solve any data analysis problem](https://www.manning.com/books/solve-any-data-analysis-problem) - [Chapter 2](https://github.com/davidasboth/solve-any-data-analysis-problem/blob/main/chapter-2/Chapter%202%20sample%20solution.ipynb)
