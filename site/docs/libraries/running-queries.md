---
sidebar_position: 3
description: The Pathling library can be used to query datasets of FHIR resources using SQL on FHIR views. This is useful for creating tabular views of FHIR data for use in analytic tools.
---

# Running queries

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

The Pathling library leverages the SQL on FHIR specification to provide a way to
project FHIR data into easy-to-use tabular forms.

Once you have transformed your FHIR data into tabular views, you can choose to
keep it in a Spark dataframe and continue to work with in Apache Spark, or
export it to Python or R dataframes or a variety of different file formats for
use in the tool of your choice.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()
data = pc.read.ndjson("/some/file/location")

result = data.view(
    resource="Patient",
    select=[
        {"column": [{"path": "getResourceKey()", "name": "patient_id"}]},
        {
            "forEach": "address",
            "column": [
                {"path": "line.join('\\n')", "name": "street"},
                {"path": "use", "name": "use"},
                {"path": "city", "name": "city"},
                {"path": "postalCode", "name": "zip"},
            ],
        },
    ],
)

display(result)
```

</TabItem>
<TabItem value="java" label="Java">

```java
package au.csiro.pathling.examples;

import static au.csiro.pathling.views.FhirView.column;
import static au.csiro.pathling.views.FhirView.columns;
import static au.csiro.pathling.views.FhirView.forEach;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.NdjsonSource;
import au.csiro.pathling.views.FhirView;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

class MyApp {

    public static void main(String[] args) {
        PathlingContext pc = PathlingContext.create();
        NdjsonSource data = pc.read()
                .ndjson("/some/file/location");

        FhirView view = FhirView.ofResource("Observation")
                .select(
                        columns(
                                column("patient_id", "getResourceKey()")
                        ),
                        forEach("code.coding",
                                column("code_system", "system"),
                                column("code_code", "code"),
                                column("code_display", "display")
                        )
                ).build();

        Dataset<Row> result = data.view(view).execute();

        result.show();
    }
}
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import au.csiro.pathling.library.PathlingContext
import au.csiro.pathling.library.io.source.NdjsonSource
import au.csiro.pathling.views.FhirView
import au.csiro.pathling.views.FhirView._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

val pc = PathlingContext.create()
val data: NdjsonSource = pc.read()
        .ndjson("/some/file/location")

val view: FhirView = FhirView.ofResource("Observation")
        .select(
            columns(
                column("patient_id", "getResourceKey()")
            ),
            forEach("code.coding",
                column("code_system", "system"),
                column("code_code", "code"),
                column("code_display", "display")
            ),
        ).build()

val result: Dataset[Row] = data.view(view).execute()

result.show()
```

</TabItem>
</Tabs>

The result of this query would look something like this:

| patient_id | street                     | use  | city       | zip   |
|------------|----------------------------|------|------------|-------|
| 1          | 398 Kautzer Walk Suite 62  | home | Barnstable | 02675 |
| 1          | 186 Nitzsche Forge         | work | Revere     | 02151 |
| 2          | 1087 Quitzon Club          | home | Plymouth   | NULL  |
| 3          | 442 Bruen Arcade           | home | Nantucket  | NULL  |
| 4          | 858 Miller Junction Apt 61 | work | Brockton   | 02301 |

For a more comprehensive example demonstrating SQL on FHIR queries with multiple
views, complex transformations and joins, see
the [SQL on FHIR example](examples/prostate-cancer.md).

