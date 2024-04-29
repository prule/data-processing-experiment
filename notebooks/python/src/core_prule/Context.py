import logging

from pyspark.sql import DataFrame

from core_prule.Configuration import SourceDefinition, Sources

logger = logging.getLogger(__name__)


class Context:
    sources: Sources
    data: dict[str:DataFrame]

    def __init__(self, sources: Sources):
        self.data = dict()
        self.sources = sources

    def put(self, key: str, value):
        self.data[key] = value

    def get(self, key: str) -> DataFrame:
        return self.data[key]

    def show(self):
        print("==============================================")
        print("Context")

        for key in self.data.keys():
            df = self.data[key]
            first_col = df.columns()[0]
            print(key + " ordered by " + first_col)
            df.orderBy(first_col).show(100, 9)

        print("==============================================")
