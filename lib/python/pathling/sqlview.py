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

from itertools import chain
from itertools import zip_longest
from uuid import uuid4
from pyspark.sql.functions import *
from pathling.sqlpath import _this

def uuid_alias():
    return "@" + uuid4().hex

def col_name(c):
    expr = c._jc.expr()
    if (expr.getClass().getName() == 'org.apache.spark.sql.catalyst.expressions.Alias'):
        return expr.name()
    else:
        raise Exception("Cannot find name")

def to_struct(df,resource_name):
    return df.select(struct(df.columns).alias(resource_name))

def Path(path):
    return lambda c:[path(c)]

def From(parent, *paths):
    def do(c):
        return list(chain(*[ p(parent(c)) for p in paths]))
    return do

def ForEach(parent, *paths):
    def do(c):
        result =  transform(parent(c), lambda e: struct(
            list(chain(*[ p(e) for p in paths]))
        ))
        return [result.alias(uuid_alias())]
    return do


def ForEachName(name,parent, *paths):
    def do(c):
        result =  transform(parent(c), lambda e: struct(
            list(chain(*[ p(e) for p in paths]))
        ))
        return [result.alias(name)]
    return do



def _form_data_view(data_source, subject_resource, joins):
    data_df = data_source.read(subject_resource)
    for join in joins:
        data_df  = data_df.join(join(data_source), col(join.master_key) == col(join.key), 'left_outer') \
            .drop(join.key)
    return to_struct(data_df, subject_resource)



def selection(struct_field):
    if struct_field.name.startswith('@'):
        return [ col(struct_field.name).getField(ssf).alias(ssf) for ssf in  struct_field.dataType.elementType.fieldNames()]
    else:
        return [col(struct_field.name)]

def flatten_df(df):
    #
    # let's try with the main level first    
    df_schema = df.schema
    nested_fields = [ f for f in df_schema.fields if f.name.startswith('@') ]
    if nested_fields:
        unnested_df = df
        for uf in nested_fields:
            unnested_df = unnested_df.withColumn(uf.name, explode_outer(uf.name))
        return flatten_df(unnested_df.select(list(chain(*[selection(f) for f in df_schema.fields]))))
    else:
        return df


def View(subject_resource, selection, joins = []):
    def do(data_source):
        view_df = _form_data_view(data_source, subject_resource, joins)
        return view_df.select(From(_this, *selection)(view_df[subject_resource]))
    return do


class ReverseView:
    def __init__(self, subject_resource, grouping_key, selection, aggs, joins = []):
        self.subject_resource = subject_resource
        self.grouping_key = grouping_key
        self.selection = selection
        self.aggs = aggs
        self.joins = joins

    @property
    def key(self):
        return self.subject_resource + "_key"

    @property
    def master_key(self):
        return "id_versioned"

    def __call__(self, data_source):

        subject_resource = self.subject_resource
        grouping_key = self.grouping_key
        selection = self.selection
        aggs = self.aggs

        data_df = _form_data_view(data_source, subject_resource, self.joins)
        agg_df = data_df.groupBy(col(subject_resource + "." + grouping_key).alias(subject_resource + "_key")) \
            .agg(struct(*[ a(c).alias(col_name(c)) for a,c in zip_longest(aggs, From(_this, *selection)(data_df[subject_resource]))]).alias(subject_resource))
        return agg_df


class JoinOneView:
    def __init__(self, subject_resource, master_key, selection, joins = []):
        self.subject_resource = subject_resource
        self.selection = selection
        self.joins = joins
        self._master_key = master_key

    @property
    def key(self):
        return self.subject_resource + "_key"

    @property
    def master_key(self):
        return self._master_key

    def __call__(self, data_source):

        subject_resource = self.subject_resource
        selection = self.selection
        data_df = _form_data_view(data_source, subject_resource, self.joins)

        view_df = data_df.select(
            col(subject_resource + ".id_versioned").alias(subject_resource + "_key"),
            struct(*[ c.alias(col_name(c)) for c in  From(_this, *selection)(data_df[subject_resource])]).alias(subject_resource)
        )
        return view_df


class JoinManyView:
    def __init__(self, subject_resource, master_resource, master_key, selection, joins = []):
        self.subject_resource = subject_resource
        self.selection = selection
        self.joins = joins
        self._master_key = master_key
        self._master_resource = master_resource

    @property
    def key(self):
        return self.subject_resource + "_key"

    @property
    def master_key(self):
        return 'id'

    def __call__(self, data_source):

        subject_resource = self.subject_resource
        selection = self.selection

        joining_df = data_source.read(self._master_resource) \
            .select(col('id').alias('master_id'), explode(self._master_key).alias('master_key'))

        data_df = _form_data_view(data_source, subject_resource, self.joins)
        joined_data_df = joining_df.join(data_df, col('master_key') == col(subject_resource + ".id_versioned"), 'inner')

        view_df = joined_data_df.groupBy(col('master_id').alias(self.key)) \
            .agg(
            map_from_entries(collect_list(struct('master_key',
                                                 struct(*From(_this, *selection)(joined_data_df[subject_resource]))))).alias(subject_resource)
        )
        return view_df


