Establishing Spark capability
====

Spark SQL lets us query CSV files in a directory as if it was a database.

First I need some data - in this oversimplified example I'm just going to pretend we have some CSV files containing bank transactions:

https://github.com/prule/data-processing-experiment/blob/80599f86db814a8e89fd50787da4433ede1add80/data/sample1/statements/2020-01.csv?plain=1#L1-L5

This gives me an easy-to-understand domain and some basic structure. See the `./data/sample1/statements/` folder.

The first thing we need is a spark session:

https://github.com/prule/data-processing-experiment/blob/8770c3265268f568645d4f230e09dcb5ef29f73f/spark/src/main/kotlin/com/example/dataprocessingexperiment/spark/Spike1.kt#L26-L28

Now we can load the statements folder into a data frame:

https://github.com/prule/data-processing-experiment/blob/8770c3265268f568645d4f230e09dcb5ef29f73f/spark/src/main/kotlin/com/example/dataprocessingexperiment/spark/Spike1.kt#L30-L39

This gives us our RAW dataframe, exactly as it is in the CSV files. Now I need to

- select only the columns I want, discarding any data that isn't needed
- type the columns appropriately

This could be done in one step, but I'm doing it as two discrete steps here to call it out as part of the logical process.

https://github.com/prule/data-processing-experiment/blob/8770c3265268f568645d4f230e09dcb5ef29f73f/spark/src/main/kotlin/com/example/dataprocessingexperiment/spark/Spike1.kt#L41-L51

https://github.com/prule/data-processing-experiment/blob/8770c3265268f568645d4f230e09dcb5ef29f73f/spark/src/main/kotlin/com/example/dataprocessingexperiment/spark/Spike1.kt#L53-L63

And there we have it - a basic runnable spark sql capability.

Running the application we can see the output from each stage:

https://github.com/prule/data-processing-experiment/blob/367140b78882922a68129400b2f16da45c7dfae1/app/part-2-sample-console-output.txt#L15-L113

You should see in the output dataframes that invalid values (those that couldn't be converted to types) come through as NULL.

Note that in order to run on Java 17 it was necessary to add some exports as JVM arguments:

https://github.com/prule/data-processing-experiment/blob/8770c3265268f568645d4f230e09dcb5ef29f73f/app/build.gradle.kts#L41-L62

This is done for the application in `app/build.gradle.kts` and for tests in `spark/build.gradle.kts`.