#  Copyright 2023 Commonwealth Scientific and Industrial Research
#  Organisation (CSIRO) ABN 41 687 119 230.
# 
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


from pyspark.sql.functions import *
from pyspark.sql.functions import _to_java_column
from pathling.udfs import subsumes, subsumed_by

def _unnest(c):
    sc = SparkContext._active_spark_context
    vf = sc._jvm.au.csiro.pathling.encoders.ValueFunctions
    return Column(vf.unnest(_to_java_column(c)))



class _ColLambda(object):

    def __init__(self, f):
        self.f = f

    def apply(self, obj):
        return _to_java_column(self.f(Column(obj)))

    class Java:
        implements = ["scala.Function1"]


def _ifArray(c, single_f, array_f):
    sc = SparkContext._active_spark_context
    vf = sc._jvm.au.csiro.pathling.encoders.ValueFunctions
    return Column(vf.ifArray(_to_java_column(c),_ColLambda(array_f),_ColLambda(single_f)))


class SQLPath:

    def __init__(self, col_f, parent = None):
        self._col_f = col_f
        self._parent = parent

    def _with(self, col_f):
        return SQLPath(col_f, self)

    def _vectorize(self, single_f, many_f):
        return self._with(lambda c: _ifArray(c, single_f, many_f))

    def count(self):
        return self._vectorize(
            lambda c: when(c.isNotNull(), 1).othewise(0),
            size
        )

    def first(self):
        return self._vectorize(
            lambda c:c,
            lambda c:c.getItem(0)
        )


    def empty(self):
        return self._vectorize(
            lambda c: c.isNull(),
            lambda c: size(c) == 0
        )

    def noT(self):
        return self._with(lambda c: ~c)

    def where(self, expr_f):
        return self._vectorize(
            lambda c:when(c.isNotNull() & expr_f(c), c).otherwise(None),
            lambda c:filter(c, lambda e:expr_f(e))
        )

    def get(self, name):
        return self._with(lambda c: c.getField(name))
        
    def alias(self, name):
        return self._with(lambda c:c.alias(name))

    def subsumedBy(self, other):
        other_path = _lit_if_needed(other)
        return self._with(lambda c:subsumed_by(c, other_path(c)))

    def subsumes(self, other):
        other_path = _lit_if_needed(other)
        return self._with(lambda c:subsumes(c, other_path(c)))

    def anyTrue(self):
        return self._vectorize(
            lambda c: c.isNotNull() & c,
            lambda c: exists(c, lambda c:c)
        )
    def allFalse(self):
        return self._vectorize(
            lambda c: c.isNull() | ~c,
            lambda c: forall(c, lambda c:~c)
        )

    def allTrue(self):
        return self._vectorize(
            lambda c: c.isNull() | c,
            lambda c: forall(c, lambda c:c)
        )

    
    def __getattr__(self, name):
        return self._with(lambda c: _unnest(c.getField(name)))

    def __eq__(self, other):
        other_path = _lit_if_needed(other)
        return SQLPath(lambda c: self(c)==other_path(c))

    def __call__(self, c = None):
        return self._col_f(self._parent(c) if self._parent else c)
    
def _resource(resource_type):
    return SQLPath(lambda c:col(resource_type))

def _lit(value):
    return SQLPath(lambda c:lit(value))
def _cons(col):
    return SQLPath(lambda c:col)

def _lit_if_needed(value):
    return value if isinstance(value, SQLPath) \
        else _cons(value) if isinstance(value, Column) \
        else _lit(value)

_ = SQLPath(lambda c:c)
_this = _
Patient = _resource('Patient')
Condition = _resource('Condition')
