Data Processing Experiment - Part 9
---
- The one where I try applying the framework to an external problem.

---

> The code for this project is available in GitHub - I’m using a branch for each part and merging each part into the **[latest](https://github.com/prule/data-processing-experiment/tree/latest)** branch. See the ReadMe.md in each branch for the story.
>
> - [Github repository for this project](https://github.com/prule/data-processing-experiment/)
> - [Pull requests for each part](https://github.com/prule/data-processing-experiment/pulls?q=is%3Apr+is%3Aclosed) 
> - [Branch for part-9](https://github.com/prule/data-processing-experiment/tree/part-9)

---

Now the basics are in place, I want to try exercising it against an external requirement and see how it holds up.

Kaggle have some inspiration in [data cleaning course](https://www.kaggle.com/learn/data-cleaning) and [Ultimate Cheatsheets: Data Cleaning](https://www.kaggle.com/code/vivovinco/ultimate-cheatsheets-data-cleaning). Here they cover things such as:
- Handling missing values
  - count the nulls per column
  - calculate the percentage of values that are null
  - remove columns that are missing values
  - fill in missing values with appropriate defaults (zero for numbers)
- Scaling and Normalization
  - scale to fit within a specific range
- Parsing Dates
- Character Encodings
- Inconsistent Data Entry
  - fixing case
  - trimming strings
  - fuzzy matching

Tableau also have a good explanation of data cleaning in their article [Guide To Data Cleaning: Definition, Benefits, Components, And How To Clean Your Data](https://www.tableau.com/learn/articles/what-is-data-cleaning)

So with this in mind, I've done the following:

*Refactoring*

- Some refactoring to remove the pipeline configuration classes and just directly instantiate the pipeline processors when loading the configuration (taking advantage of polymorphic serialization and removing a lot of unnecessary code!)

*Empty count statistic*

- Added EmptyCount statistic - This counts the empty values for each column (or just the columns you specify). "Empty" means different things depending on the data type of the column - For numbers it can be NULL or NaN. For strings it could be NULL, or a blank string, or whitespace.  
  - This also adds an "EmptyPercentage" statistic, calculated from the number of empty values as a percentage of the total number of values.

*Summary statistic*

- Added summary statistic - this is based off the spark dataset.summary() feature which return statistics like mean, min, max, stddev, count, percentiles etc.

*Trimming whitespace*

- Added the capability to specify that a column should be trimmed when loading the "selected" dataset - both at the column level and at the table level.
- Adding some spaces to the sample data shows how whitespace interferes 
```
|2020-01-01|      1|    burger  |            Gympie  | 15.45|TRANSACTION|        NULL|
|2020-01-02|      1|      movie |      Southern Downs| 20.00|TRANSACTION|        NULL|
|2020-01-03|      1|      tennis|            Banana  | 35.00|TRANSACTION|        NULL|
|2020-01-04|      2|      petrol|   Central Highlands|150.45|TRANSACTION|        NULL|
|2020-02-01|      1|      burger|            Yarrabah| 15.46|TRANSACTION|  Queensland|
```
- When adding `trim=true` to the description column we get
```
|2020-01-01|      1|     burger|            Gympie  | 15.45|TRANSACTION|        NULL|
|2020-01-02|      1|      movie|      Southern Downs| 20.00|TRANSACTION|        NULL|
|2020-01-03|      1|     tennis|            Banana  | 35.00|TRANSACTION|        NULL|
|2020-01-04|      2|     petrol|   Central Highlands|150.45|TRANSACTION|  Queensland|
|2020-02-01|      1|     burger|            Yarrabah| 15.46|TRANSACTION|  Queensland|
```
- When adding `trim=true` to the whole table we get
```
|2020-01-01|      1|     burger|           Gympie| 15.45|TRANSACTION|  Queensland|
|2020-01-02|      1|      movie|   Southern Downs| 20.00|TRANSACTION|  Queensland|
|2020-01-03|      1|     tennis|           Banana| 35.00|TRANSACTION|  Queensland|
|2020-01-04|      2|     petrol|Central Highlands|150.45|TRANSACTION|  Queensland|
|2020-02-01|      1|     burger|         Yarrabah| 15.46|TRANSACTION|  Queensland|
```
- Removing the whitespaces by trimming description and location now fixes the issue making the data cleaner and more consistent.

