import logging

from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import trim, col, to_date, coalesce

from core_prule.Configuration import SourceDefinition, ColumnDefinition

logger = logging.getLogger(__name__)


class DataFrameBuilder:
    source: SourceDefinition
    root_path: str
    spark: SparkSession

    def __init__(
            self,
            source: SourceDefinition,
            root_path: str,
            spark_session: SparkSession
    ):
        self.source = source
        self.root_path = root_path
        self.spark = spark_session

    def raw(self) -> DataFrame:
        table = self.source.table
        path = self.root_path + self.source.path

        logger.debug("Loading table " + self.source.key + " from " + path)

        return (self.spark.read.format(self.source.type)
                .option("header", "true")
                .option("sep", table.delimiter_or_default()).load(path)
                .alias(self.source.name))

    def selected(self) -> DataFrame:
        raw = self.raw()
        columns = list(map(lambda x: self.to_selected_column(x, raw), self.source.table.columns))
        return raw.select(columns)

    def to_selected_column(self, column: ColumnDefinition, raw: DataFrame) -> Column:
        c: Column = None
        for name in column.names:
            if name in raw.columns:
                if self.source.table.should_trim(name):
                    c = trim(col(name))
                else:
                    c = col(name)
                break

        if c is None:
            raise Exception("Could not find any of the columns " + str(
                column.names) + " on table " + self.source.key + " at " + self.source.path)

        return c.alias(column.alias)

    def typed(self) -> DataFrame:
        selected = self.selected()
        columns = list(map(lambda x: self.to_typed_column(x, selected), self.source.table.columns))
        return selected.select(columns)

    def to_typed_column(self, column: ColumnDefinition, raw: DataFrame) -> Column:
        match column.type.type:
            case "com.example.dataprocessingexperiment.spark.data.types.IntegerType":
                c = col(column.alias).cast("integer")
            case "com.example.dataprocessingexperiment.spark.data.types.DecimalType":
                c = col(column.alias).cast(f"decimal({column.type.precision},{column.type.scale})")
            case "com.example.dataprocessingexperiment.spark.data.types.DateType":
                d = trim(col(column.alias))
                if column.type.formats:
                    c = coalesce(*list(map(lambda x: to_date(d, x), column.type.formats)))
                else:
                    c = col(column.alias).cast("date")
            case "com.example.dataprocessingexperiment.spark.data.types.BooleanType":
                c = col(column.alias).cast("boolean")
            case _:
                c = col(column.alias)

        return c.alias(column.alias)
