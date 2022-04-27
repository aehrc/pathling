from pyspark.sql.functions import *


class PathlingContext:

    def __init__(self, serverUrl):
        self._serverUrl = serverUrl

    def memberOf(self, df, codingColumn, valueSetUrl, outputColumnName):
        return df.withColumn(outputColumnName, lit(True))
