Data Processing Experiment - Part 8
---
- The one where I experiment with task pipelines


---

> The code for this project is available in GitHub - I’m using a branch for each part and merging each part into the **[latest](https://github.com/prule/data-processing-experiment/tree/latest)** branch. See the ReadMe.md in each branch for the story.
>
> - [Github repository for this project](https://github.com/prule/data-processing-experiment/)
> - [Pull requests for each part](https://github.com/prule/data-processing-experiment/pulls?q=is%3Apr+is%3Aclosed) 
> - [Branch for part-8](https://github.com/prule/data-processing-experiment/tree/part-8)

Pipelines
---
I'm not confident about what I did in the last part by adding the union field to the source. This won't work for other functionality - what we really need is a list of transforms to perform. These could go on the table source - if it only needs to operate on itself - or it could be separated out into it's own space and run across all tables accessible via the context.

So this has been refactored now. Instead what I have is a PipelineConfiguration which is a list of tasks to perform. In this case I want to:

1. Union the 3 LGA dataframes into one called `lgas`
2. Add a column to the `transactions` dataframe called `type` with the literal value `TRANSACTION`
3. Join `transactions` to `lgas` on `transactions.location=lgas.level_2_name` resulting in a dataframe called `transactionsWithLGAs` and has all the columns from `transactions` plus `lgas.level_1_name`.
4. Output the `transactionsWithLGAs` table to CSV

```json5
{
  id: "p1",
  name: "name",
  description: "description",
  tasks: [
    {
      type: "com.example.dataprocessingexperiment.tables.pipeline.UnionTaskDefinition",
      id: "t1",
      name: "name",
      description: "description",
      destination: "lgas",
      // Union tables "lga-1" and "lga-2" and "lga-3" together into table "lgas"
      tables: [
        "lga-1",
        "lga-2",
        "lga-3",
      ]
    },
    {
      type: "com.example.dataprocessingexperiment.tables.pipeline.LiteralTaskDefinition",
      id: "t2",
      name: "name",
      description: "description",
      table: "transactions",
      // add literal columns with name -> value mapping
      columns: {
        // add a column "type" with literal value "TRANSACTION"
        "type": "TRANSACTION"
      },
    },
    {
      type: "com.example.dataprocessingexperiment.tables.pipeline.JoinTaskDefinition",
      id: "t3",
      name: "name",
      description: "description",
      // join table "transactions" to "lgas" using transactions.location=lgas.level_2_name
      // include columns from t1, add lgas.level_1_name and store this in "transactionsWithLGAs" (we could store as transactions if we want to override the original transactions dataframe with the joined result)
      table1: "transactions",
      table2: "lgas",
      destination: "transactionsWithLGAs",
      // join "on" definition
      on: {
        "location": "level_2_name"
      },
      // include these columns from lgas
      columns: [
        "level_1_name"
      ],
      joinType: "left"
    },
    {
      type: "com.example.dataprocessingexperiment.tables.pipeline.OutputTaskDefinition",
      id: "t4",
      name: "name",
      description: "description",
      table: "transactionsWithLGAs",
      path: "./build/output/sample1/transactions",
      format: "csv",
      mode: "overwrite",
      options: {
        "header": "true",
      }
      // TODO add partition capability
    }
  ]
}
```

For this:
- Each task gets a definition class so the configuration can be read into a class that represents its configuration eg JoinTaskDefinition, LiteralTaskDefinition... (extending AbstractTaskDefinition)
- Use [polymorphic serialization](https://github.com/Kotlin/kotlinx.serialization/blob/master/docs/polymorphism.md) to read the configuration into the right classes during deserialization.
  - This needs the serializers module configuring with a mapping of class to serializer
```kotlin
val pipelineConfigurationRepository = PipelineConfigurationRepository(
    SerializersModule {
        polymorphic(AbstractTaskDefinition::class, JoinTaskDefinition::class, JoinTaskDefinition.serializer())
        polymorphic(AbstractTaskDefinition::class, UnionTaskDefinition::class, UnionTaskDefinition.serializer())
        polymorphic(AbstractTaskDefinition::class, LiteralTaskDefinition::class, LiteralTaskDefinition.serializer())
        polymorphic(AbstractTaskDefinition::class, OutputTaskDefinition::class, OutputTaskDefinition.serializer())
    }
)
```
> During serializing and de-serializing it uses a `type` property in the JSON to identify the required class
> ```json5
> type: "com.example.dataprocessingexperiment.tables.pipeline.JoinTaskDefinition",
>```

- Now load the configuration and process the tasks. 
```kotlin
val pipelineConfiguration = pipelineConfigurationRepository.load(
    File("./src/main/resources/sample1.pipeline.json5").inputStream()
)

PipelineProcessor(pipelineConfiguration).process(context)
```

The PipelineProcessor works by iterating over the task definitions - using a task registry to find the processor class to use for a task definition. It requires the context which was built from the table definitions so it can access the loaded dataframes.

```kotlin
    fun process(context: SparkContext) {
        logger.info { "Starting pipeline id=${configuration.id} name=${configuration.name}" }
        configuration.tasks.forEach { task ->
            logger.info { "Applying task id=${task.id} name=${task.name}"}
            val processor = taskRegistry.processor(task.javaClass.kotlin)
            logger.info { "Starting processor $processor" }
            processor.process(context, task)
            logger.info { "Finished processor $processor" }
        }
    }
```
And the processor in question knows how to apply logic based on the task definition. For example:

```kotlin
class LiteralProcessor : Processor {
    fun process(context: SparkContext, literalDefinition: LiteralTaskDefinition) {

        // get the required table from the context
        var table = context.get(literalDefinition.table)

        // for each entry in the columns mapping, add the columns with the literal value 
        literalDefinition.columns.map {
            table = table.withColumn(it.key, functions.lit(it.value))
        }

        // replace the table in the context
        context.set(literalDefinition.table, table)
    }

    override fun process(context: SparkContext, task: AbstractTaskDefinition) {
        process(context, task as LiteralTaskDefinition)
    }
}
```

The pipeline task registry provides a means to find the processor for a task definition, and allows registration of more processors along with the default provided ones.

```kotlin
class PipelineTaskRegistry {
  // map of provided tasks to processors
  private val taskMap = mapOf(
    JoinTaskDefinition::class to JoinProcessor::class,
    UnionTaskDefinition::class to UnionProcessor::class,
    LiteralTaskDefinition::class to LiteralProcessor::class,
    OutputTaskDefinition::class to OutputProcessor::class
  )

  private val externalTasks: MutableMap<KClass<out AbstractTaskDefinition>, KClass<out Processor>> = mutableMapOf()

  /**
   * Register a new processor for a given definition.
   */
  fun add(definition: KClass<out AbstractTaskDefinition>, processor: KClass<out Processor>) {
    externalTasks.put(definition, processor)
  }

  /**
   * Returns an instance of the Processor registered for the give task definition.
   */
  fun processor(id: KClass<AbstractTaskDefinition>): Processor {
    // gives preference to externally registered tasks so the defaults can be overridden
    val map = if (externalTasks.containsKey(id)) { externalTasks } else { taskMap }
    // instantiate the processor
    return map[id]!!.java.constructors.first { it.parameterCount == 0 }.newInstance() as Processor
  }
}
```
When we run this pipeline we get a CSV containing valid transactions enriched with the "level_1_name" corresponding to "location"

|date|account|description| location          |amount|type| level_1_name |
|----|-------|-----------|-------------------|------|----|--------------|
|2020-03-01|1|burger| Greater Dandenong |15.47|TRANSACTION| Victoria     |
|2020-02-04|2|petrol| Gannawarra        |150.46|TRANSACTION| Victoria     |
|2020-01-01|1|burger| Gympie            |15.45|TRANSACTION| Queensland   |
|2020-02-03|1|tennis| Maroondah         |35.01|TRANSACTION| Victoria     |
|2020-01-02|1|movie| Southern Downs    |20.00|TRANSACTION| Queensland   |
|2020-02-02|1|movie| Barcaldine        |20.01|TRANSACTION| Queensland   |
|2020-01-03|1|tennis| Banana            |35.00|TRANSACTION| Queensland   |
|2020-02-01|1|burger| Yarrabah          |15.46|TRANSACTION| Queensland   |
|2020-01-04|2|petrol| Central Highlands |150.45|TRANSACTION| Queensland   |
|2020-02-04|2|electricity| Hepburn           |300.47|TRANSACTION| Victoria     |
|2020-03-03|1|tennis| Maroondah         |35.03|TRANSACTION| Victoria     |
|2020-03-04|2|petrol|                   |150.47|TRANSACTION|              |

> Note the last row is one we want to keep - it has an amount spent, which is essential information. Even though location is missing, we still need to track the expense - even though we can't allocate it to a location. So in this case we'd just want to report it as location unknown.

So now we have a pipeline capability, it would be possible to utilise this functionality at different stages (raw, selected, typed, valid) of loading the data. The advantage of this is that if we wanted to reuse a table across multiple ETL pipelines, then we could reuse the table definition AND the transforms for this table. 

> The tables module has gone through a bit of change - classes have been renamed to '*Definition' since they are only configuration objects with no logic. It also hosts not only table configuration but also statistics and pipelines. There's 2 options here:
> 1. rename "tables" module to "configuration" module and leave everything there
> 2. split out into multiple modules to reflect how standalone and optional they are. 
>
> Because of the small number of classes I'll go with option 1 for now.


> I'm starting to see a higher level abstraction that could add value here. We have a [fact table](https://en.wikipedia.org/wiki/Fact_table) (transactions) with a ([hierarchical](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/fixed-depth-hierarchy/)) [dimension](https://en.wikipedia.org/wiki/Dimension_(data_warehouse)) (LGAs) - it would be feasible to create tooling that could be configured to generate the JSON configuration (a pipeline to do the join and de-normalise) - if this was useful... 

I think the next thing to do now is try to validate the framework by applying it to a more realistic example - Can I use it to ETL some real data? And if so, what problems do I encounter?

---

Here's the output from the [reference application](https://github.com/prule/data-processing-experiment/blob/part-8/app/src/main/kotlin/com/example/dataprocessingexperiment/app/App.kt) at this stage:

```text
> Task :app:run
Starting...

Raw dataset

root
 |-- date: string (nullable = true)
 |-- account: string (nullable = true)
 |-- description: string (nullable = true)
 |-- amount: string (nullable = true)
 |-- location: string (nullable = true)
 |-- comment: string (nullable = true)

+------------+-------+------------+-------+-----------------+--------------------+
|        date|account| description| amount|         location|             comment|
+------------+-------+------------+-------+-----------------+--------------------+
|        NULL|      x|      tennis|   0.03|             NULL|             no date|
|  01-03-2020|      1|      burger|  15.47|Greater Dandenong|alternative date ...|
|  03-03-2020|      1|      tennis|  35.03|        Maroondah|alternative date ...|
|  04-03-2020|      2|      petrol| 150.47|             NULL|alternative date ...|
|  2020-01-01|      1|      burger|  15.45|           Gympie|                NULL|
|  2020-01-02|      1|       movie|  20.00|   Southern Downs|                NULL|
|  2020-01-03|      1|      tennis|  35.00|           Banana|                NULL|
|  2020-01-04|      2|      petrol| 150.45|Central Highlands|                NULL|
|  2020-01-04|      2|      petrol| 150.45|Central Highlands|                NULL|
|  2020-02-01|      1|      burger|  15.46|         Yarrabah|                NULL|
|  2020-02-02|      1|       movie|  20.01|       Barcaldine|                NULL|
|  2020-02-03|      1|      tennis|  35.01|        Maroondah|                NULL|
|  2020-02-04|      2|      petrol| 150.46|       Gannawarra|                NULL|
|  2020-02-04|      2| electricity| 300.47|          Hepburn|                NULL|
|  2020-12-01|       |      tennis|   0.04|             NULL| blank (many spac...|
|  2020-12-01|      x|      petrol|      x|             NULL| invalid number f...|
|  2020-13-01|      x|      burger|   0.01| unknown location|        invalid date|
|invalid date|      x|      petrol|   0.02|                 |        invalid date|
|           x|      x|           x|      x|             NULL| row with multipl...|
|           x|      x|           x|      x|             NULL| row with multipl...|
+------------+-------+------------+-------+-----------------+--------------------+

row count = 20

RAW Statistics

root
 |-- key: string (nullable = true)
 |-- column: string (nullable = true)
 |-- discriminator: string (nullable = true)
 |-- value: string (nullable = true)

+-------------------+-------+-------------+-----+
|                key| column|discriminator|value|
+-------------------+-------+-------------+-----+
|       CountByMonth|   date|         NULL|    8|
|       CountByMonth|   date|      2020-01|    5|
|       CountByMonth|   date|      2020-02|    5|
|       CountByMonth|   date|      2020-12|    2|
|       CountByValue|account|         NULL|    1|
|       CountByValue|account|            1|    8|
|       CountByValue|account|            2|    5|
|       CountByValue|account|            x|    6|
|duplicate row count|   NULL|         NULL|    4|
|                max| amount|         NULL|    x|
|                min| amount|         NULL| 0.01|
|          row count|   NULL|         NULL|   20|
+-------------------+-------+-------------+-----+

row count = 12

SELECTED dataset

root
 |-- date: string (nullable = true)
 |-- account: string (nullable = true)
 |-- description: string (nullable = true)
 |-- location: string (nullable = true)
 |-- amount: string (nullable = true)

+------------+-------+------------+-----------------+-------+
|        date|account| description|         location| amount|
+------------+-------+------------+-----------------+-------+
|        NULL|      x|      tennis|             NULL|   0.03|
|  01-03-2020|      1|      burger|Greater Dandenong|  15.47|
|  03-03-2020|      1|      tennis|        Maroondah|  35.03|
|  04-03-2020|      2|      petrol|             NULL| 150.47|
|  2020-01-01|      1|      burger|           Gympie|  15.45|
|  2020-01-02|      1|       movie|   Southern Downs|  20.00|
|  2020-01-03|      1|      tennis|           Banana|  35.00|
|  2020-01-04|      2|      petrol|Central Highlands| 150.45|
|  2020-01-04|      2|      petrol|Central Highlands| 150.45|
|  2020-02-01|      1|      burger|         Yarrabah|  15.46|
|  2020-02-02|      1|       movie|       Barcaldine|  20.01|
|  2020-02-03|      1|      tennis|        Maroondah|  35.01|
|  2020-02-04|      2|      petrol|       Gannawarra| 150.46|
|  2020-02-04|      2| electricity|          Hepburn| 300.47|
|  2020-12-01|       |      tennis|             NULL|   0.04|
|  2020-12-01|      x|      petrol|             NULL|      x|
|  2020-13-01|      x|      burger| unknown location|   0.01|
|invalid date|      x|      petrol|                 |   0.02|
|           x|      x|           x|             NULL|      x|
|           x|      x|           x|             NULL|      x|
+------------+-------+------------+-----------------+-------+

row count = 20

Typed dataset

root
 |-- date: date (nullable = true)
 |-- account: string (nullable = true)
 |-- description: string (nullable = true)
 |-- location: string (nullable = true)
 |-- amount: decimal(10,2) (nullable = true)

+----------+-------+------------+-----------------+------+
|      date|account| description|         location|amount|
+----------+-------+------------+-----------------+------+
|      NULL|      x|      burger| unknown location|  0.01|
|      NULL|      x|      petrol|                 |  0.02|
|      NULL|      x|      tennis|             NULL|  0.03|
|      NULL|      x|           x|             NULL|  NULL|
|      NULL|      x|           x|             NULL|  NULL|
|2020-01-01|      1|      burger|           Gympie| 15.45|
|2020-01-02|      1|       movie|   Southern Downs| 20.00|
|2020-01-03|      1|      tennis|           Banana| 35.00|
|2020-01-04|      2|      petrol|Central Highlands|150.45|
|2020-01-04|      2|      petrol|Central Highlands|150.45|
|2020-02-01|      1|      burger|         Yarrabah| 15.46|
|2020-02-02|      1|       movie|       Barcaldine| 20.01|
|2020-02-03|      1|      tennis|        Maroondah| 35.01|
|2020-02-04|      2|      petrol|       Gannawarra|150.46|
|2020-02-04|      2| electricity|          Hepburn|300.47|
|2020-03-01|      1|      burger|Greater Dandenong| 15.47|
|2020-03-03|      1|      tennis|        Maroondah| 35.03|
|2020-03-04|      2|      petrol|             NULL|150.47|
|2020-12-01|       |      tennis|             NULL|  0.04|
|2020-12-01|      x|      petrol|             NULL|  NULL|
+----------+-------+------------+-----------------+------+

row count = 20

Valid dataset

root
 |-- date: date (nullable = true)
 |-- account: string (nullable = true)
 |-- description: string (nullable = true)
 |-- location: string (nullable = true)
 |-- amount: decimal(10,2) (nullable = true)

+----------+-------+------------+-----------------+------+
|      date|account| description|         location|amount|
+----------+-------+------------+-----------------+------+
|2020-01-01|      1|      burger|           Gympie| 15.45|
|2020-01-02|      1|       movie|   Southern Downs| 20.00|
|2020-01-03|      1|      tennis|           Banana| 35.00|
|2020-01-04|      2|      petrol|Central Highlands|150.45|
|2020-02-01|      1|      burger|         Yarrabah| 15.46|
|2020-02-02|      1|       movie|       Barcaldine| 20.01|
|2020-02-03|      1|      tennis|        Maroondah| 35.01|
|2020-02-04|      2|      petrol|       Gannawarra|150.46|
|2020-02-04|      2| electricity|          Hepburn|300.47|
|2020-03-01|      1|      burger|Greater Dandenong| 15.47|
|2020-03-03|      1|      tennis|        Maroondah| 35.03|
|2020-03-04|      2|      petrol|             NULL|150.47|
+----------+-------+------------+-----------------+------+

row count = 12

VALID Statistics

root
 |-- key: string (nullable = true)
 |-- column: string (nullable = true)
 |-- discriminator: string (nullable = true)
 |-- value: string (nullable = true)

+-------------------+-------+-------------+------+
|                key| column|discriminator| value|
+-------------------+-------+-------------+------+
|       CountByMonth|   date|      2020-01|     4|
|       CountByMonth|   date|      2020-02|     5|
|       CountByMonth|   date|      2020-03|     3|
|       CountByValue|account|            1|     8|
|       CountByValue|account|            2|     4|
|duplicate row count|   NULL|         NULL|     0|
|                max| amount|         NULL|300.47|
|                min| amount|         NULL| 15.45|
|          row count|   NULL|         NULL|    12|
+-------------------+-------+-------------+------+

row count = 9

Raw dataset

root
 |-- Code State: string (nullable = true)
 |-- Name State: string (nullable = true)
 |-- Year: string (nullable = true)
 |-- Code Local Government Area: string (nullable = true)
 |-- Iso 3166-3 Area Code: string (nullable = true)
 |-- Name Local Government Area: string (nullable = true)
 |-- Type: string (nullable = true)
 |-- Long Name Local Government Area: string (nullable = true)

+----------+---------------+----+--------------------------+--------------------+--------------------------+--------------------+-------------------------------+
|Code State|     Name State|Year|Code Local Government Area|Iso 3166-3 Area Code|Name Local Government Area|                Type|Long Name Local Government Area|
+----------+---------------+----+--------------------------+--------------------+--------------------------+--------------------+-------------------------------+
|         1|New South Wales|2021|                     10650|                 AUS|                  Berrigan|local government ...|                       Berrigan|
|         1|New South Wales|2021|                     10650|                 AUS|                  Berrigan|local government ...|                       Berrigan|
|         1|New South Wales|2021|                     10850|                 AUS|                   Blayney|local government ...|                        Blayney|
|         1|New South Wales|2021|                     11500|                 AUS|              Campbelltown|local government ...|             Campbelltown (NSW)|
|         1|New South Wales|2021|                     11700|                 AUS|           Central Darling|local government ...|                Central Darling|
|         1|New South Wales|2021|                     15990|                 AUS|          Northern Beaches|local government ...|               Northern Beaches|
|         1|New South Wales|2021|                     11300|                 AUS|                   Burwood|local government ...|                        Burwood|
|         1|New South Wales|2021|                     11750|                 AUS|                     Cobar|local government ...|                          Cobar|
|         1|New South Wales|2021|                     14000|                 AUS|                   Hornsby|local government ...|                        Hornsby|
|         1|New South Wales|2021|                     14750|                 AUS|                    Leeton|local government ...|                         Leeton|
|         1|New South Wales|2021|                     17200|                 AUS|                    Sydney|local government ...|                         Sydney|
+----------+---------------+----+--------------------------+--------------------+--------------------------+--------------------+-------------------------------+

row count = 11

RAW Statistics

root
 |-- key: string (nullable = true)
 |-- column: string (nullable = true)
 |-- discriminator: string (nullable = true)
 |-- value: string (nullable = true)

+-------------------+------+-------------+-----+
|                key|column|discriminator|value|
+-------------------+------+-------------+-----+
|duplicate row count|  NULL|         NULL|    2|
|          row count|  NULL|         NULL|   11|
+-------------------+------+-------------+-----+

row count = 2

SELECTED dataset

root
 |-- level_1_code: string (nullable = true)
 |-- level_1_name: string (nullable = true)
 |-- level_2_code: string (nullable = true)
 |-- level_2_name: string (nullable = true)

+------------+---------------+------------+----------------+
|level_1_code|   level_1_name|level_2_code|    level_2_name|
+------------+---------------+------------+----------------+
|           1|New South Wales|       10650|        Berrigan|
|           1|New South Wales|       10650|        Berrigan|
|           1|New South Wales|       10850|         Blayney|
|           1|New South Wales|       11500|    Campbelltown|
|           1|New South Wales|       11700| Central Darling|
|           1|New South Wales|       15990|Northern Beaches|
|           1|New South Wales|       11300|         Burwood|
|           1|New South Wales|       11750|           Cobar|
|           1|New South Wales|       14000|         Hornsby|
|           1|New South Wales|       14750|          Leeton|
|           1|New South Wales|       17200|          Sydney|
+------------+---------------+------------+----------------+

row count = 11

Typed dataset

root
 |-- level_1_code: string (nullable = true)
 |-- level_1_name: string (nullable = true)
 |-- level_2_code: string (nullable = true)
 |-- level_2_name: string (nullable = true)

+------------+---------------+------------+----------------+
|level_1_code|   level_1_name|level_2_code|    level_2_name|
+------------+---------------+------------+----------------+
|           1|New South Wales|       10650|        Berrigan|
|           1|New South Wales|       10650|        Berrigan|
|           1|New South Wales|       10850|         Blayney|
|           1|New South Wales|       11500|    Campbelltown|
|           1|New South Wales|       11700| Central Darling|
|           1|New South Wales|       15990|Northern Beaches|
|           1|New South Wales|       11300|         Burwood|
|           1|New South Wales|       11750|           Cobar|
|           1|New South Wales|       14000|         Hornsby|
|           1|New South Wales|       14750|          Leeton|
|           1|New South Wales|       17200|          Sydney|
+------------+---------------+------------+----------------+

row count = 11

Valid dataset

root
 |-- level_1_code: string (nullable = true)
 |-- level_1_name: string (nullable = true)
 |-- level_2_code: string (nullable = true)
 |-- level_2_name: string (nullable = true)

+------------+---------------+------------+----------------+
|level_1_code|   level_1_name|level_2_code|    level_2_name|
+------------+---------------+------------+----------------+
|           1|New South Wales|       11700| Central Darling|
|           1|New South Wales|       11500|    Campbelltown|
|           1|New South Wales|       17200|          Sydney|
|           1|New South Wales|       11750|           Cobar|
|           1|New South Wales|       15990|Northern Beaches|
|           1|New South Wales|       10650|        Berrigan|
|           1|New South Wales|       11300|         Burwood|
|           1|New South Wales|       14750|          Leeton|
|           1|New South Wales|       10850|         Blayney|
|           1|New South Wales|       14000|         Hornsby|
+------------+---------------+------------+----------------+

row count = 10

VALID Statistics

root
 |-- key: string (nullable = true)
 |-- column: string (nullable = true)
 |-- discriminator: string (nullable = true)
 |-- value: string (nullable = true)

+-------------------+------+-------------+-----+
|                key|column|discriminator|value|
+-------------------+------+-------------+-----+
|duplicate row count|  NULL|         NULL|    0|
|          row count|  NULL|         NULL|   10|
+-------------------+------+-------------+-----+

row count = 2

Raw dataset

root
 |-- Official Name State: string (nullable = true)
 |-- Year: string (nullable = true)
 |-- Official Code Local Government Area: string (nullable = true)
 |-- Official Code State: string (nullable = true)
 |-- Iso 3166-3 Area Code: string (nullable = true)
 |-- Official Name Local Government Area: string (nullable = true)

+-------------------+----+-----------------------------------+-------------------+--------------------+-----------------------------------+
|Official Name State|Year|Official Code Local Government Area|Official Code State|Iso 3166-3 Area Code|Official Name Local Government Area|
+-------------------+----+-----------------------------------+-------------------+--------------------+-----------------------------------+
|           Victoria|2021|                              21750|                  2|                 AUS|                        Colac Otway|
|           Victoria|2021|                              22670|                  2|                 AUS|                  Greater Dandenong|
|           Victoria|2021|                              22910|                  2|                 AUS|                            Hepburn|
|           Victoria|2021|                              24410|                  2|                 AUS|                          Maroondah|
|           Victoria|2021|                              24850|                  2|                 AUS|                           Mitchell|
|           Victoria|2021|                              23350|                  2|                 AUS|                             Indigo|
|           Victoria|2021|                              26610|                  2|                 AUS|                          Swan Hill|
|           Victoria|2021|                              22250|                  2|                 AUS|                         Gannawarra|
|           Victoria|2021|                              20830|                  2|                 AUS|                            Baw Baw|
|           Victoria|2021|                              21110|                  2|                 AUS|                         Boroondara|
+-------------------+----+-----------------------------------+-------------------+--------------------+-----------------------------------+

row count = 10

RAW Statistics

root
 |-- key: string (nullable = true)
 |-- column: string (nullable = true)
 |-- discriminator: string (nullable = true)
 |-- value: string (nullable = true)

+-------------------+------+-------------+-----+
|                key|column|discriminator|value|
+-------------------+------+-------------+-----+
|duplicate row count|  NULL|         NULL|    0|
|          row count|  NULL|         NULL|   10|
+-------------------+------+-------------+-----+

row count = 2

SELECTED dataset

root
 |-- level_1_code: string (nullable = true)
 |-- level_1_name: string (nullable = true)
 |-- level_2_code: string (nullable = true)
 |-- level_2_name: string (nullable = true)

+------------+------------+------------+-----------------+
|level_1_code|level_1_name|level_2_code|     level_2_name|
+------------+------------+------------+-----------------+
|           2|    Victoria|       21750|      Colac Otway|
|           2|    Victoria|       22670|Greater Dandenong|
|           2|    Victoria|       22910|          Hepburn|
|           2|    Victoria|       24410|        Maroondah|
|           2|    Victoria|       24850|         Mitchell|
|           2|    Victoria|       23350|           Indigo|
|           2|    Victoria|       26610|        Swan Hill|
|           2|    Victoria|       22250|       Gannawarra|
|           2|    Victoria|       20830|          Baw Baw|
|           2|    Victoria|       21110|       Boroondara|
+------------+------------+------------+-----------------+

row count = 10

Typed dataset

root
 |-- level_1_code: string (nullable = true)
 |-- level_1_name: string (nullable = true)
 |-- level_2_code: string (nullable = true)
 |-- level_2_name: string (nullable = true)

+------------+------------+------------+-----------------+
|level_1_code|level_1_name|level_2_code|     level_2_name|
+------------+------------+------------+-----------------+
|           2|    Victoria|       21750|      Colac Otway|
|           2|    Victoria|       22670|Greater Dandenong|
|           2|    Victoria|       22910|          Hepburn|
|           2|    Victoria|       24410|        Maroondah|
|           2|    Victoria|       24850|         Mitchell|
|           2|    Victoria|       23350|           Indigo|
|           2|    Victoria|       26610|        Swan Hill|
|           2|    Victoria|       22250|       Gannawarra|
|           2|    Victoria|       20830|          Baw Baw|
|           2|    Victoria|       21110|       Boroondara|
+------------+------------+------------+-----------------+

row count = 10

Valid dataset

root
 |-- level_1_code: string (nullable = true)
 |-- level_1_name: string (nullable = true)
 |-- level_2_code: string (nullable = true)
 |-- level_2_name: string (nullable = true)

+------------+------------+------------+-----------------+
|level_1_code|level_1_name|level_2_code|     level_2_name|
+------------+------------+------------+-----------------+
|           2|    Victoria|       24410|        Maroondah|
|           2|    Victoria|       22910|          Hepburn|
|           2|    Victoria|       21110|       Boroondara|
|           2|    Victoria|       22250|       Gannawarra|
|           2|    Victoria|       21750|      Colac Otway|
|           2|    Victoria|       26610|        Swan Hill|
|           2|    Victoria|       24850|         Mitchell|
|           2|    Victoria|       22670|Greater Dandenong|
|           2|    Victoria|       23350|           Indigo|
|           2|    Victoria|       20830|          Baw Baw|
+------------+------------+------------+-----------------+

row count = 10

VALID Statistics

root
 |-- key: string (nullable = true)
 |-- column: string (nullable = true)
 |-- discriminator: string (nullable = true)
 |-- value: string (nullable = true)

+-------------------+------+-------------+-----+
|                key|column|discriminator|value|
+-------------------+------+-------------+-----+
|duplicate row count|  NULL|         NULL|    0|
|          row count|  NULL|         NULL|   10|
+-------------------+------+-------------+-----+

row count = 2

Raw dataset

root
 |-- Official Name State: string (nullable = true)
 |-- Official Code Local Government Area: string (nullable = true)
 |-- Official Code State: string (nullable = true)
 |-- Iso 3166-3 Area Code: string (nullable = true)
 |-- Official Name Local Government Area: string (nullable = true)

+-------------------+-----------------------------------+-------------------+--------------------+-----------------------------------+
|Official Name State|Official Code Local Government Area|Official Code State|Iso 3166-3 Area Code|Official Name Local Government Area|
+-------------------+-----------------------------------+-------------------+--------------------+-----------------------------------+
|         Queensland|                              30410|                  3|                 AUS|                         Barcaldine|
|         Queensland|                              33620|                  3|                 AUS|                             Gympie|
|         Queensland|                              35670|                  3|                 AUS|                           Napranum|
|         Queensland|                              36660|                  3|                 AUS|                     Southern Downs|
|         Queensland|                              30370|                  3|                 AUS|                             Banana|
|         Queensland|                              33100|                  3|                 AUS|                          Etheridge|
|         Queensland|                              34830|                  3|                 AUS|                             Mapoon|
|         Queensland|                              35300|                  3|                 AUS|                          Mount Isa|
|         Queensland|                              37600|                  3|                 AUS|                           Yarrabah|
|         Queensland|                              32270|                  3|                 AUS|                  Central Highlands|
+-------------------+-----------------------------------+-------------------+--------------------+-----------------------------------+

row count = 10

RAW Statistics

root
 |-- key: string (nullable = true)
 |-- column: string (nullable = true)
 |-- discriminator: string (nullable = true)
 |-- value: string (nullable = true)

+-------------------+------+-------------+-----+
|                key|column|discriminator|value|
+-------------------+------+-------------+-----+
|duplicate row count|  NULL|         NULL|    0|
|          row count|  NULL|         NULL|   10|
+-------------------+------+-------------+-----+

row count = 2

SELECTED dataset

root
 |-- level_1_code: string (nullable = true)
 |-- level_1_name: string (nullable = true)
 |-- level_2_code: string (nullable = true)
 |-- level_2_name: string (nullable = true)

+------------+------------+------------+-----------------+
|level_1_code|level_1_name|level_2_code|     level_2_name|
+------------+------------+------------+-----------------+
|           3|  Queensland|       30410|       Barcaldine|
|           3|  Queensland|       33620|           Gympie|
|           3|  Queensland|       35670|         Napranum|
|           3|  Queensland|       36660|   Southern Downs|
|           3|  Queensland|       30370|           Banana|
|           3|  Queensland|       33100|        Etheridge|
|           3|  Queensland|       34830|           Mapoon|
|           3|  Queensland|       35300|        Mount Isa|
|           3|  Queensland|       37600|         Yarrabah|
|           3|  Queensland|       32270|Central Highlands|
+------------+------------+------------+-----------------+

row count = 10

Typed dataset

root
 |-- level_1_code: string (nullable = true)
 |-- level_1_name: string (nullable = true)
 |-- level_2_code: string (nullable = true)
 |-- level_2_name: string (nullable = true)

+------------+------------+------------+-----------------+
|level_1_code|level_1_name|level_2_code|     level_2_name|
+------------+------------+------------+-----------------+
|           3|  Queensland|       30410|       Barcaldine|
|           3|  Queensland|       33620|           Gympie|
|           3|  Queensland|       35670|         Napranum|
|           3|  Queensland|       36660|   Southern Downs|
|           3|  Queensland|       30370|           Banana|
|           3|  Queensland|       33100|        Etheridge|
|           3|  Queensland|       34830|           Mapoon|
|           3|  Queensland|       35300|        Mount Isa|
|           3|  Queensland|       37600|         Yarrabah|
|           3|  Queensland|       32270|Central Highlands|
+------------+------------+------------+-----------------+

row count = 10

Valid dataset

root
 |-- level_1_code: string (nullable = true)
 |-- level_1_name: string (nullable = true)
 |-- level_2_code: string (nullable = true)
 |-- level_2_name: string (nullable = true)

+------------+------------+------------+-----------------+
|level_1_code|level_1_name|level_2_code|     level_2_name|
+------------+------------+------------+-----------------+
|           3|  Queensland|       30370|           Banana|
|           3|  Queensland|       35670|         Napranum|
|           3|  Queensland|       32270|Central Highlands|
|           3|  Queensland|       37600|         Yarrabah|
|           3|  Queensland|       30410|       Barcaldine|
|           3|  Queensland|       34830|           Mapoon|
|           3|  Queensland|       33100|        Etheridge|
|           3|  Queensland|       36660|   Southern Downs|
|           3|  Queensland|       33620|           Gympie|
|           3|  Queensland|       35300|        Mount Isa|
+------------+------------+------------+-----------------+

row count = 10

VALID Statistics

root
 |-- key: string (nullable = true)
 |-- column: string (nullable = true)
 |-- discriminator: string (nullable = true)
 |-- value: string (nullable = true)

+-------------------+------+-------------+-----+
|                key|column|discriminator|value|
+-------------------+------+-------------+-----+
|duplicate row count|  NULL|         NULL|    0|
|          row count|  NULL|         NULL|   10|
+-------------------+------+-------------+-----+

row count = 2
15:45:36.595 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Starting pipeline id=p1 name=name
15:45:36.595 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Applying task id=t1 name=name
15:45:36.595 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Starting processor com.example.dataprocessingexperiment.spark.pipeline.UnionProcessor@59c08452
15:45:36.600 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Finished processor com.example.dataprocessingexperiment.spark.pipeline.UnionProcessor@59c08452
15:45:36.600 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Applying task id=t2 name=name
15:45:36.600 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Starting processor com.example.dataprocessingexperiment.spark.pipeline.LiteralProcessor@24d92ffc
15:45:36.602 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Finished processor com.example.dataprocessingexperiment.spark.pipeline.LiteralProcessor@24d92ffc
15:45:36.602 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Applying task id=t3 name=name
15:45:36.602 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Starting processor com.example.dataprocessingexperiment.spark.pipeline.JoinProcessor@788cd72f
15:45:36.608 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Finished processor com.example.dataprocessingexperiment.spark.pipeline.JoinProcessor@788cd72f
15:45:36.608 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Applying task id=t4 name=name
15:45:36.608 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Starting processor com.example.dataprocessingexperiment.spark.pipeline.OutputProcessor@319f3eb2
15:45:36.917 [main] INFO  c.e.d.s.pipeline.PipelineProcessor MDC= - Finished processor com.example.dataprocessingexperiment.spark.pipeline.OutputProcessor@319f3eb2
==============================================
Context
transactions
+----------+-------+------------+-----------------+------+-----------+
|      date|account| description|         location|amount|       type|
+----------+-------+------------+-----------------+------+-----------+
|2020-03-01|      1|      burger|Greater Dandenong| 15.47|TRANSACTION|
|2020-02-04|      2|      petrol|       Gannawarra|150.46|TRANSACTION|
|2020-01-01|      1|      burger|           Gympie| 15.45|TRANSACTION|
|2020-02-03|      1|      tennis|        Maroondah| 35.01|TRANSACTION|
|2020-01-02|      1|       movie|   Southern Downs| 20.00|TRANSACTION|
|2020-02-02|      1|       movie|       Barcaldine| 20.01|TRANSACTION|
|2020-01-03|      1|      tennis|           Banana| 35.00|TRANSACTION|
|2020-02-01|      1|      burger|         Yarrabah| 15.46|TRANSACTION|
|2020-01-04|      2|      petrol|Central Highlands|150.45|TRANSACTION|
|2020-02-04|      2| electricity|          Hepburn|300.47|TRANSACTION|
|2020-03-03|      1|      tennis|        Maroondah| 35.03|TRANSACTION|
|2020-03-04|      2|      petrol|             NULL|150.47|TRANSACTION|
+----------+-------+------------+-----------------+------+-----------+

lga-1
+------------+---------------+------------+----------------+
|level_1_code|   level_1_name|level_2_code|    level_2_name|
+------------+---------------+------------+----------------+
|           1|New South Wales|       11700| Central Darling|
|           1|New South Wales|       11500|    Campbelltown|
|           1|New South Wales|       17200|          Sydney|
|           1|New South Wales|       11750|           Cobar|
|           1|New South Wales|       15990|Northern Beaches|
|           1|New South Wales|       10650|        Berrigan|
|           1|New South Wales|       11300|         Burwood|
|           1|New South Wales|       14750|          Leeton|
|           1|New South Wales|       10850|         Blayney|
|           1|New South Wales|       14000|         Hornsby|
+------------+---------------+------------+----------------+

lga-2
+------------+------------+------------+-----------------+
|level_1_code|level_1_name|level_2_code|     level_2_name|
+------------+------------+------------+-----------------+
|           2|    Victoria|       24410|        Maroondah|
|           2|    Victoria|       22910|          Hepburn|
|           2|    Victoria|       21110|       Boroondara|
|           2|    Victoria|       22250|       Gannawarra|
|           2|    Victoria|       21750|      Colac Otway|
|           2|    Victoria|       26610|        Swan Hill|
|           2|    Victoria|       24850|         Mitchell|
|           2|    Victoria|       22670|Greater Dandenong|
|           2|    Victoria|       23350|           Indigo|
|           2|    Victoria|       20830|          Baw Baw|
+------------+------------+------------+-----------------+

lga-3
+------------+------------+------------+-----------------+
|level_1_code|level_1_name|level_2_code|     level_2_name|
+------------+------------+------------+-----------------+
|           3|  Queensland|       30370|           Banana|
|           3|  Queensland|       35670|         Napranum|
|           3|  Queensland|       32270|Central Highlands|
|           3|  Queensland|       37600|         Yarrabah|
|           3|  Queensland|       30410|       Barcaldine|
|           3|  Queensland|       34830|           Mapoon|
|           3|  Queensland|       33100|        Etheridge|
|           3|  Queensland|       36660|   Southern Downs|
|           3|  Queensland|       33620|           Gympie|
|           3|  Queensland|       35300|        Mount Isa|
+------------+------------+------------+-----------------+

lgas
+------------+---------------+------------+-----------------+
|level_1_code|   level_1_name|level_2_code|     level_2_name|
+------------+---------------+------------+-----------------+
|           1|New South Wales|       11700|  Central Darling|
|           1|New South Wales|       11500|     Campbelltown|
|           1|New South Wales|       17200|           Sydney|
|           1|New South Wales|       11750|            Cobar|
|           1|New South Wales|       15990| Northern Beaches|
|           1|New South Wales|       10650|         Berrigan|
|           1|New South Wales|       11300|          Burwood|
|           1|New South Wales|       14750|           Leeton|
|           1|New South Wales|       10850|          Blayney|
|           1|New South Wales|       14000|          Hornsby|
|           2|       Victoria|       24410|        Maroondah|
|           2|       Victoria|       22910|          Hepburn|
|           2|       Victoria|       21110|       Boroondara|
|           2|       Victoria|       22250|       Gannawarra|
|           2|       Victoria|       21750|      Colac Otway|
|           2|       Victoria|       26610|        Swan Hill|
|           2|       Victoria|       24850|         Mitchell|
|           2|       Victoria|       22670|Greater Dandenong|
|           2|       Victoria|       23350|           Indigo|
|           2|       Victoria|       20830|          Baw Baw|
|           3|     Queensland|       30370|           Banana|
|           3|     Queensland|       35670|         Napranum|
|           3|     Queensland|       32270|Central Highlands|
|           3|     Queensland|       37600|         Yarrabah|
|           3|     Queensland|       30410|       Barcaldine|
|           3|     Queensland|       34830|           Mapoon|
|           3|     Queensland|       33100|        Etheridge|
|           3|     Queensland|       36660|   Southern Downs|
|           3|     Queensland|       33620|           Gympie|
|           3|     Queensland|       35300|        Mount Isa|
+------------+---------------+------------+-----------------+

transactionsWithLGAs
+----------+-------+------------+-----------------+------+-----------+------------+
|      date|account| description|         location|amount|       type|level_1_name|
+----------+-------+------------+-----------------+------+-----------+------------+
|2020-03-01|      1|      burger|Greater Dandenong| 15.47|TRANSACTION|    Victoria|
|2020-02-04|      2|      petrol|       Gannawarra|150.46|TRANSACTION|    Victoria|
|2020-01-01|      1|      burger|           Gympie| 15.45|TRANSACTION|  Queensland|
|2020-02-03|      1|      tennis|        Maroondah| 35.01|TRANSACTION|    Victoria|
|2020-01-02|      1|       movie|   Southern Downs| 20.00|TRANSACTION|  Queensland|
|2020-02-02|      1|       movie|       Barcaldine| 20.01|TRANSACTION|  Queensland|
|2020-01-03|      1|      tennis|           Banana| 35.00|TRANSACTION|  Queensland|
|2020-02-01|      1|      burger|         Yarrabah| 15.46|TRANSACTION|  Queensland|
|2020-01-04|      2|      petrol|Central Highlands|150.45|TRANSACTION|  Queensland|
|2020-02-04|      2| electricity|          Hepburn|300.47|TRANSACTION|    Victoria|
|2020-03-03|      1|      tennis|        Maroondah| 35.03|TRANSACTION|    Victoria|
|2020-03-04|      2|      petrol|             NULL|150.47|TRANSACTION|        NULL|
+----------+-------+------------+-----------------+------+-----------+------------+

==============================================
Finished...
```