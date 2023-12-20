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
from pathling.functions import *

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

    def __init__(self, col_f, parent = None, agg_f = None):
        self._col_f = col_f
        self._parent = parent
        self._agg_f = agg_f

    def _with(self, col_f, agg_f = None):
        return SQLPath(col_f, self, agg_f)

    def _vectorize(self, single_f, many_f, agg_f = None):
        return self._with(lambda c: _ifArray(c, single_f, many_f), agg_f)

    def count(self):
        return self._vectorize(
            lambda c: when(c.isNotNull(), 1).othewise(0),
            size, 
            sum
        )

    def first(self):
        return self._vectorize(
            lambda c:c,
            lambda c:c.getItem(0),
            first
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

    def _getField(self, name):
        return self._with(lambda c: c.getField(name))
        
    def subsumedBy(self, other):
        other_path = _lit_if_needed(other)
        return self._with(lambda c:subsumed_by(c, other_path(c)))

    def subsumes(self, other):
        other_path = _lit_if_needed(other)
        return self._with(lambda c:subsumes(c, other_path(c)))

    def anyTrue(self):
        return self._vectorize(
            lambda c: c.isNotNull() & c,
            lambda c: exists(c, lambda c:c),
            max
        )
    def allFalse(self):
        return self._vectorize(
            lambda c: c.isNull() | ~c,
            lambda c: forall(c, lambda c:~c),
            min
        )

    def allTrue(self):
        return self._vectorize(
            lambda c: c.isNull() | c,
            lambda c: forall(c, lambda c:c)
        )

    def get(self, name):
        return self._with(lambda c: _unnest(c.getField(name)), 
                          agg_f = lambda ac: _unnest(collect_list(ac)))

    
    def __getattr__(self, name):
        return self.get(name)

    def __eq__(self, other):
        other_path = _lit_if_needed(other)
        return SQLPath(lambda c: self(c)==other_path(c))


    def __and__(self, other):
        other_path = _lit_if_needed(other)
        return SQLPath(lambda c: self(c) & other_path(c))

    def __call__(self, c = None, agg = False):
        col_result =  self._col_f(self._parent(c) if self._parent else self._col_f(c))
        return col_result if not agg else self._agg(col_result)
    
    def _agg(self, c):
        if self._agg_f:
            return self._agg_f(c) 
        else: 
            raise Exception("Cannot aggregate")
    
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

_ = SQLPath(lambda c:c, agg_f=count)
_this = _
Patient = _resource('Patient')
Condition = _resource('Condition')

def coding(code, system):
    return to_coding(lit(code), system)
