from pyspark.sql.functions import *


def _context(_jvm):
    return _jvm.au.csiro.pathling.api.PathlingContext


class PathlingContext:
    def __init__(self, sparkSession, serverUrl):
        self._jctx = _context(sparkSession._jvm).create(serverUrl)

    def memberOf(self, df, codingColumn, valueSetUrl, outputColumnName):
        return df.withColumn(outputColumnName, lit(True))
