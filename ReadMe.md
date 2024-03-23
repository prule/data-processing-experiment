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

Kaggle have a [data cleaning course](https://www.kaggle.com/learn/data-cleaning) that offers some inspiration. In this course they:
- Handle missing values
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

Data can be found [here](https://www.kaggle.com/code/alexisbcook/inconsistent-data-entry/data).

Tableau have a good explanation of data cleaning in their article [Guide To Data Cleaning: Definition, Benefits, Components, And How To Clean Your Data](https://www.tableau.com/learn/articles/what-is-data-cleaning)

