Data Processing Experiment - Part 8
---
- The one where I experiment with task pipelines


---

> The code for this project is available in GitHub - I’m using a branch for each part and merging each part into the **[latest](https://github.com/prule/data-processing-experiment/tree/latest)** branch. See the ReadMe.md in each branch for the story.
>
> - [Github repository for this project](https://github.com/prule/data-processing-experiment/)
> - [Pull requests for each part](https://github.com/prule/data-processing-experiment/pulls?q=is%3Apr+is%3Aclosed) 
> - [Branch for part-8](https://github.com/prule/data-processing-experiment/tree/part-7)

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
- Use polymorphic serialization to read the configuration into the right classes during deserialization.
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

|date|account|description|location|amount|type|level_1_name|
|----|-------|-----------|--------|------|----|------------|
|2020-03-01|1|burger|Greater Dandenong|15.47|TRANSACTION|Victoria|
|2020-02-04|2|petrol|Gannawarra|150.46|TRANSACTION|Victoria|
|2020-01-01|1|burger|Gympie|15.45|TRANSACTION|Queensland|
|2020-02-03|1|tennis|Maroondah|35.01|TRANSACTION|Victoria|
|2020-01-02|1|movie|Southern Downs|20.00|TRANSACTION|Queensland|
|2020-02-02|1|movie|Barcaldine|20.01|TRANSACTION|Queensland|
|2020-01-03|1|tennis|Banana|35.00|TRANSACTION|Queensland|
|2020-02-01|1|burger|Yarrabah|15.46|TRANSACTION|Queensland|
|2020-01-04|2|petrol|Central Highlands|150.45|TRANSACTION|Queensland|
|2020-02-04|2|electricity|Hepburn|300.47|TRANSACTION|Victoria|
|2020-03-03|1|tennis|Maroondah|35.03|TRANSACTION|Victoria|
|2020-03-04|2|petrol||150.47|TRANSACTION||