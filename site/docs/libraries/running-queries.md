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
data = pc.read.ndjson("s3://somebucket/synthea/ndjson")

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
import au.csiro.pathling.library.PathlingContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import au.csiro.pathling.library.io.source.NdjsonSource;

class MyApp {

    public static void main(String[] args) {
        PathlingContext pc = PathlingContext.create();
        NdjsonSource data = pc.read.ndjson("s3://somebucket/synthea/ndjson");

        Dataset<Row> result = data.view(ResourceType.PATIENT)
                .json("{\n" +
                        "  \"resource\": \"Patient\",\n" +
                        "  \"select\": [\n" +
                        "    {\n" +
                        "      \"column\": [\n" +
                        "        {\n" +
                        "          \"path\": \"getResourceKey()\",\n" +
                        "          \"name\": \"patient_id\"\n" +
                        "        }\n" +
                        "      ]\n" +
                        "    },\n" +
                        "    {\n" +
                        "      \"forEach\": \"address\",\n" +
                        "      \"column\": [\n" +
                        "        {\n" +
                        "          \"path\": \"line.join('\\\\n')\",\n" +
                        "          \"name\": \"street\"\n" +
                        "        },\n" +
                        "        {\n" +
                        "          \"path\": \"use\",\n" +
                        "          \"name\": \"use\"\n" +
                        "        },\n" +
                        "        {\n" +
                        "          \"path\": \"city\",\n" +
                        "          \"name\": \"city\"\n" +
                        "        },\n" +
                        "        {\n" +
                        "          \"path\": \"postalCode\",\n" +
                        "          \"name\": \"zip\"\n" +
                        "        }\n" +
                        "      ]\n" +
                        "    }\n" +
                        "  ]\n" +
                        "}")
                .execute();

        result.show();
    }
}
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import au.csiro.pathling.library.PathlingContext
import org.hl7.fhir.r4.model.Enumerations.ResourceType

val pc = PathlingContext.create()
val data = pc.read.ndjson("s3://somebucket/synthea/ndjson")

val result = data.view(ResourceType.PATIENT)
        .json("""{
    "resource": "Patient",
    "select": [
      {
        "column": [
          {
            "path": "getResourceKey()",
            "name": "patient_id"
          }
        ]
      },
      {
        "forEach": "address",
        "column": [
          {
            "path": "line.join('\\n')",
            "name": "street"
          },
          {
            "path": "use",
            "name": "use"
          },
          {
            "path": "city",
            "name": "city"
          },
          {
            "path": "postalCode",
            "name": "zip"
          }
        ]
      }
    ]
  }""")
        .execute()

display(result)
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

