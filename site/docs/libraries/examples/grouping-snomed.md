---
sidebar_position: 3
title: Grouping and analysing SNOMED CT data
description: Tutorial demonstrating how to group and analyse SNOMED CT data using Python with the Pathling library and a terminology server.
---

<iframe width="560" height="315" src="https://www.youtube.com/embed/ahVHCWpXSPA?si=pDKHIt7_wceB270D" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>

# Grouping and analysing SNOMED CT data

This tutorial demonstrates how to group and analyse SNOMED CT data using
Python with the Pathling library and a terminology server. The process involves
setting up a Python environment, loading and transforming data, creating value
sets for grouping, and visualising the results.

## Setting up the environment

The first step is to set up a Python environment in Visual Studio Code. This
involves importing the necessary libraries: `pathling` for SNOMED data
operations, `pyspark` for data manipulation, and `plotly` for data
visualization.

```python
from pathling import PathlingContext
from pyspark.sql.functions import col, month
import plotly.express as px
```

## Configuring Pathling

A Pathling context is created and configured to use a local terminology cache.
This helps to speed up the analysis by reusing terminology requests.

```python
pc = PathlingContext.create()
```

## Loading and transforming data

The source data is loaded from a CSV file, and new columns are added for '
Arrival Month' and 'Primary Diagnosis Term' by retrieving preferred terms from
the terminology server.

```python
source_df = pc.spark.read.csv("snomed_data.csv", header=True)

transformed_df = source_df.select(
    col("id"),
    month(col("arrive_at")).alias("Arrival Month"),
    col("primary_diagnosis_concept")
).withColumn(
    "Primary Diagnosis Term",
    pc.snomed.display(col("primary_diagnosis_concept"))
)
```

## Grouping with value sets

The core of the grouping process involves defining value sets based on SNOMED
codes. The video provides examples for creating value sets for "Viral
Infection", "Musculoskeletal Injury", and "Mental Health Problem" using
Expression Constraint Language (ECL). The Shrimp terminology browser is used to
explore SNOMED hierarchies and create these ECL expressions.

```python
viral_infection_ecl = "<< 64572001"  # Viral disease
musculoskeletal_injury_ecl = "<< 263534002"  # Injury of musculoskeletal system
mental_health_ecl = "<< 40733004 |Mental state finding|"

categorized_df = transformed_df.withColumn(
    "Viral Infection",
    pc.snomed.member_of(col("primary_diagnosis_concept"), viral_infection_ecl)
).withColumn(
    "Musculoskeletal Injury",
    pc.snomed.member_of(col("primary_diagnosis_concept"),
                        musculoskeletal_injury_ecl)
).withColumn(
    "Mental Health Problem",
    pc.snomed.member_of(col("primary_diagnosis_concept"), mental_health_ecl)
)
```

## Handling overlapping categories

Due to the nature of SNOMED's multiple parentage, some categories may overlap.
For instance, a condition like "rabies coma" can be classified as both a viral
infection and a mental health finding. The data is grouped by 'Arrival Month' to
count all encounters and then visualised using Plotly to show the number of
encounters per month for each category.

```python
all_encounters_df = categorized_df.groupBy(
    "Arrival Month").count().withColumnRenamed("count", "All Encounters")
viral_infection_count_df = categorized_df.filter(
    col("Viral Infection")).groupBy("Arrival Month").count().withColumnRenamed(
    "count", "Viral Infection")
musculoskeletal_injury_count_df = categorized_df.filter(
    col("Musculoskeletal Injury")).groupBy(
    "Arrival Month").count().withColumnRenamed("count",
                                               "Musculoskeletal Injury")
mental_health_count_df = categorized_df.filter(
    col("Mental Health Problem")).groupBy(
    "Arrival Month").count().withColumnRenamed("count", "Mental Health Problem")

# Code for joining these dataframes and creating a Plotly chart would follow
```

## Creating mutually exclusive categories

To avoid overlapping categories, a hierarchy is created. "Viral Infection" is
the first category, followed by "Musculoskeletal Injury," which excludes any
codes already in the "Viral Infection" category. The "Mental Health Problem"
category then excludes any codes already present in the previous two categories.
An "Other" category is also created for any remaining codes.

```python
mutually_exclusive_df = categorized_df.withColumn(
    "Category",
    when(col("Viral Infection"), "Viral Infection")
    .when(~col("Viral Infection") & col("Musculoskeletal Injury"),
          "Musculoskeletal Injury")
    .when(~col("Viral Infection") & ~col("Musculoskeletal Injury") & col(
        "Mental Health Problem"), "Mental Health Problem")
    .otherwise("Other")
)
```

## Visualisation of mutually exclusive categories

import GroupedSnomed from '@site/src/images/snomed-grouped.png';

<img src={GroupedSnomed} alt="Grouped bar chart showing mutually exclusive categories" width="800"/>

The mutually exclusive categories are then counted and visualised in a grouped
bar chart, showing the proportional distribution of these categories over time.

```python
mutually_exclusive_counts_df = mutually_exclusive_df.groupBy("Arrival Month",
                                                             "Category").count()

fig = px.bar(
    mutually_exclusive_counts_df.toPandas(),
    x="Arrival Month",
    y="count",
    color="Category",
    barmode="group"
)
fig.show()
```

For more detailed information on grouping SNOMED data, you can refer
to [Terminology functions](/docs/libraries/terminology.md).
