import logging
from pyspark.sql import SparkSession, DataFrame

from core_prule.Configuration import Sources
from core_prule.Context import Context
from core_prule.DataFrameBuilder import DataFrameBuilder
from core_prule.JsonRepository import JsonRepository

logger = logging.getLogger(__name__)


class App:
    context: Context

    # def __init__(self):

    def go(self):
        sources = Sources.from_dict(JsonRepository().load_file('./config/sample1/sample1.tables.json5'))

        with (SparkSession.builder.appName("Data Processing Experiment").master("local").getOrCreate()) as spark:
            context = Context(sources)

            for source in sources.sources:
                builder = DataFrameBuilder(source, "./data/", spark)

                # ------------
                # RAW
                # ------------

                # get the raw version of the dataset, everything is a string, and all columns are included
                raw = builder.raw()
                self.display("raw", raw)

                # ------------
                # SELECTED
                # ------------
                #
                # Get the selected version of the dataset, everything is a string,
                # and only configured columns are included.
                # Values will be trimmed if specified, and columns will be aliased.
                selected = builder.selected()
                self.display("selected", selected)

                # ------------
                # TYPED
                # ------------
                #
                # get the typed version of the dataset, with columns and types specified in config
                typed = builder.typed()
                self.display("typed", typed)

                # Add to context
                context.put(source.key, typed)

    def display(self, name: str, df: DataFrame):
        print()
        print(name)
        print()

        df.printSchema()
        df.sort(df.columns[0]).show(100, 10)
        print(f"Row count = {df.count()}")


if __name__ == "__main__":
    app = App()
    app.go()
