Data Processing Experiment - Part 8
---
- The one where I experiment with task pipelines


---

> The code for this project is available in GitHub - I’m using a branch for each part and merging each part into the **[latest](https://github.com/prule/data-processing-experiment/tree/latest)** branch. See the ReadMe.md in each branch for the story.
>
> - [Github repository for this project](https://github.com/prule/data-processing-experiment/)
> - [Pull requests for each part](https://github.com/prule/data-processing-experiment/pulls?q=is%3Apr+is%3Aclosed) 
> - [Branch for part-8](https://github.com/prule/data-processing-experiment/tree/part-7)

I'm not confident about what I did in the last part by adding the union field to the source. This won't work for other functionality - what we really need is a list of transforms to perform. These could go on the table source - if it only needs to operate on itself - or it could be separated out into it's own space and run across all tables accessible via the context.

