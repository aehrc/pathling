---
sidebar_position: 6
title: Recursive traversal with repeat
description: Example of using the repeat directive to flatten nested QuestionnaireResponse items.
---

# Recursive traversal with repeat

This example demonstrates how to use the `repeat` directive in SQL on FHIR views to recursively traverse and flatten nested data structures. We'll use QuestionnaireResponse resources, which commonly contain deeply nested item hierarchies.

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

## Understanding the repeat directive

The `repeat` directive enables automatic flattening of hierarchical FHIR data to any depth by recursively following specified paths. This is particularly useful for structures like:

- QuestionnaireResponse items (nested questions and answers)
- Extension hierarchies (nested extensions)
- Organization hierarchies (nested part-of relationships)

Unlike `forEach`, which only unnests a single level, `repeat` traverses all levels of nesting.

## Setup

First, initialise the Pathling context:

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(pathling)

pc <- pathling_connect()
```

</TabItem>
</Tabs>

## Load QuestionnaireResponse data

For this example, we'll read QuestionnaireResponse resources that contain nested items:

<Tabs>
<TabItem value="python" label="Python">

```python
data = pc.read.ndjson("/path/to/questionnaire-responses")
```

</TabItem>
<TabItem value="r" label="R">

```r
data <- pc %>% pathling_read_ndjson("/path/to/questionnaire-responses")
```

</TabItem>
</Tabs>

## Flattening nested items with repeat

QuestionnaireResponse resources can have deeply nested item structures, where items can contain nested items to represent sub-questions or grouped questions. The `repeat` directive allows us to flatten all items regardless of their nesting depth.

<Tabs>
<TabItem value="python" label="Python">

```python
flattened_items = data.view(
    resource="QuestionnaireResponse",
    select=[
        {
            "column": [
                {
                    "name": "id",
                    "path": "getResourceKey()",
                    "description": "Unique questionnaire response identifier"
                },
                {
                    "name": "questionnaire",
                    "path": "questionnaire",
                    "description": "Canonical URL of the questionnaire"
                },
                {
                    "name": "patient_id",
                    "path": "subject.getReferenceKey(Patient)",
                    "description": "Patient identifier"
                },
                {
                    "name": "authored",
                    "path": "authored",
                    "description": "Date and time the response was authored"
                }
            ]
        },
        {
            "repeat": ["item"],
            "column": [
                {
                    "name": "item_link_id",
                    "path": "linkId",
                    "description": "Unique identifier for this item"
                },
                {
                    "name": "item_text",
                    "path": "text",
                    "description": "Question text"
                },
                {
                    "name": "answer_value_string",
                    "path": "answer.value.ofType(string)",
                    "description": "String answer value"
                },
                {
                    "name": "answer_value_integer",
                    "path": "answer.value.ofType(integer)",
                    "description": "Integer answer value"
                },
                {
                    "name": "answer_value_boolean",
                    "path": "answer.value.ofType(boolean)",
                    "description": "Boolean answer value"
                },
                {
                    "name": "answer_value_date",
                    "path": "answer.value.ofType(date)",
                    "description": "Date answer value"
                }
            ]
        }
    ]
)
```

</TabItem>
<TabItem value="r" label="R">

```r
flattened_items <- data %>%
    ds_view(
        resource = "QuestionnaireResponse",
        select = list(
            list(
                column = list(
                    list(
                        name = "id",
                        path = "getResourceKey()",
                        description = "Unique questionnaire response identifier"
                    ),
                    list(
                        name = "questionnaire",
                        path = "questionnaire",
                        description = "Canonical URL of the questionnaire"
                    ),
                    list(
                        name = "patient_id",
                        path = "subject.getReferenceKey(Patient)",
                        description = "Patient identifier"
                    ),
                    list(
                        name = "authored",
                        path = "authored",
                        description = "Date and time the response was authored"
                    )
                )
            ),
            list(
                repeat = list("item"),
                column = list(
                    list(
                        name = "item_link_id",
                        path = "linkId",
                        description = "Unique identifier for this item"
                    ),
                    list(
                        name = "item_text",
                        path = "text",
                        description = "Question text"
                    ),
                    list(
                        name = "answer_value_string",
                        path = "answer.value.ofType(string)",
                        description = "String answer value"
                    ),
                    list(
                        name = "answer_value_integer",
                        path = "answer.value.ofType(integer)",
                        description = "Integer answer value"
                    ),
                    list(
                        name = "answer_value_boolean",
                        path = "answer.value.ofType(boolean)",
                        description = "Boolean answer value"
                    ),
                    list(
                        name = "answer_value_date",
                        path = "answer.value.ofType(date)",
                        description = "Date answer value"
                    )
                )
            )
        )
    )
```

</TabItem>
</Tabs>

View the results:

<Tabs>
<TabItem value="python" label="Python">

```python
flattened_items.show(10, truncate=False)
```

</TabItem>
<TabItem value="r" label="R">

```r
flattened_items %>% head(10) %>% collect()
```

</TabItem>
</Tabs>

The result will contain one row per item at any nesting level:

| id                  | questionnaire                        | patient_id  | authored                  | item_link_id | item_text            | answer_value_string | answer_value_integer | answer_value_boolean | answer_value_date |
|---------------------|--------------------------------------|-------------|---------------------------|--------------|----------------------|---------------------|----------------------|----------------------|-------------------|
| QuestionnaireRes... | http://example.org/q/phq9            | Patient/123 | 2024-03-15T10:30:00+10:00 | 1            | Little interest...   | NULL                | 2                    | NULL                 | NULL              |
| QuestionnaireRes... | http://example.org/q/phq9            | Patient/123 | 2024-03-15T10:30:00+10:00 | 2            | Feeling down...      | NULL                | 1                    | NULL                 | NULL              |
| QuestionnaireRes... | http://example.org/q/health-history  | Patient/456 | 2024-03-16T14:20:00+10:00 | demographics | Demographics         | NULL                | NULL                 | NULL                 | NULL              |
| QuestionnaireRes... | http://example.org/q/health-history  | Patient/456 | 2024-03-16T14:20:00+10:00 | name         | Full name            | John Smith          | NULL                 | NULL                 | NULL              |
| QuestionnaireRes... | http://example.org/q/health-history  | Patient/456 | 2024-03-16T14:20:00+10:00 | dob          | Date of birth        | NULL                | NULL                 | NULL                 | 1980-05-22        |
| QuestionnaireRes... | http://example.org/q/health-history  | Patient/456 | 2024-03-16T14:20:00+10:00 | conditions   | Medical conditions   | NULL                | NULL                 | NULL                 | NULL              |
| QuestionnaireRes... | http://example.org/q/health-history  | Patient/456 | 2024-03-16T14:20:00+10:00 | diabetes     | Diabetes             | NULL                | NULL                 | TRUE                 | NULL              |
| QuestionnaireRes... | http://example.org/q/health-history  | Patient/456 | 2024-03-16T14:20:00+10:00 | hypertension | Hypertension         | NULL                | NULL                 | FALSE                | NULL              |

Note how all items are flattened into a single table, regardless of their nesting depth. Items that are group items (with no answer) and items with answers are all represented as separate rows.

## Filtering nested items

You can combine the `repeat` directive with `where` clauses to filter the results. For example, to only include items with answers:

<Tabs>
<TabItem value="python" label="Python">

```python
answered_items = data.view(
    resource="QuestionnaireResponse",
    select=[
        {
            "column": [
                {
                    "name": "id",
                    "path": "getResourceKey()",
                    "description": "Response identifier"
                },
                {
                    "name": "patient_id",
                    "path": "subject.getReferenceKey(Patient)",
                    "description": "Patient identifier"
                }
            ]
        },
        {
            "repeat": ["item"],
            "column": [
                {
                    "name": "item_link_id",
                    "path": "linkId",
                    "description": "Item identifier"
                },
                {
                    "name": "answer_value",
                    "path": "answer.value.ofType(string)",
                    "description": "String answer"
                }
            ]
        }
    ],
    where=[
        {
            "description": "Only items with string answers",
            "path": "item.exists(answer.value.ofType(string).exists())"
        }
    ]
)
```

</TabItem>
<TabItem value="r" label="R">

```r
answered_items <- data %>%
    ds_view(
        resource = "QuestionnaireResponse",
        select = list(
            list(
                column = list(
                    list(
                        name = "id",
                        path = "getResourceKey()",
                        description = "Response identifier"
                    ),
                    list(
                        name = "patient_id",
                        path = "subject.getReferenceKey(Patient)",
                        description = "Patient identifier"
                    )
                )
            ),
            list(
                repeat = list("item"),
                column = list(
                    list(
                        name = "item_link_id",
                        path = "linkId",
                        description = "Item identifier"
                    ),
                    list(
                        name = "answer_value",
                        path = "answer.value.ofType(string)",
                        description = "String answer"
                    )
                )
            )
        ),
        where = list(
            list(
                description = "Only items with string answers",
                path = "item.exists(answer.value.ofType(string).exists())"
            )
        )
    )
```

</TabItem>
</Tabs>

## Comparison with forEach

The key difference between `repeat` and `forEach`:

- **forEach**: Unnests a single level of a collection (e.g., `forEach: "address"` creates one row per address)
- **repeat**: Recursively traverses nested structures to any depth (e.g., `repeat: ["item"]` creates one row per item at any nesting level)

Use `forEach` when you have a simple collection to unnest. Use `repeat` when you have recursive or deeply nested structures.

## Other use cases

The `repeat` directive can be used with any recursive FHIR structure:

**Extension hierarchies:**

```python
{
    "repeat": ["extension"],
    "column": [
        {"name": "url", "path": "url"},
        {"name": "value", "path": "value.ofType(string)"}
    ]
}
```

**Organisation hierarchies:**

```python
{
    "repeat": ["partOf.resolve()"],
    "column": [
        {"name": "org_id", "path": "getResourceKey()"},
        {"name": "org_name", "path": "name"}
    ]
}
```

The `repeat` directive provides a powerful way to work with hierarchical FHIR data, making it easy to analyse deeply nested structures using familiar tabular operations.

## Understanding traversal depth limits

The `repeat` directive includes a configurable depth limit to prevent infinite recursion in truly self-referential structures. However, it's important to understand when this limit applies and when it doesn't.

### When the depth limit applies

Pathling uses a **type-aware depth limiting mechanism**. The depth counter only decrements when traversing to a node of the **same type** as its parent. This means the limit only applies to structures where the schema is genuinely identical at each level.

The primary use case where this applies is **extension hierarchies**, where `extension.extension` has exactly the same schema as its parent:

```python
{
    "repeat": ["extension"],
    "column": [
        {"name": "url", "path": "url"},
        {"name": "value", "path": "value.ofType(string)"}
    ]
}
```

In this case, because each `extension.extension` has the same schema, the depth limit prevents infinite recursion if there were circular references or excessively deep nesting.

### When the depth limit does NOT apply

For **QuestionnaireResponse items** (the focus of this example), the depth limit does **not** apply during traversal. Although `item.item.item...` appears recursive, each level of nesting actually has a different schema in Pathling's internal representation. The type-aware mechanism recognises this and allows traversal to any depth without consuming the depth budget.

This means:

- You can flatten QuestionnaireResponse items to any depth without worrying about the `max_unbound_traversal_depth` limit
- The limit is only relevant when working with genuinely self-referential structures like extensions

### Configuring the depth limit

If you're working with deeply nested extensions or other truly self-referential structures, you can configure the depth limit using the `max_unbound_traversal_depth` parameter:

<Tabs>
<TabItem value="python" label="Python">

```python
pc = PathlingContext.create(
    max_unbound_traversal_depth=20  # Default is 10
)
```

</TabItem>
<TabItem value="r" label="R">

```r
pc <- pathling_connect(
    max_unbound_traversal_depth = 20  # Default is 10
)
```

</TabItem>
</Tabs>

### Why this limit exists

The depth limit exists as a safety mechanism to prevent:

- Infinite loops during query planning
- Memory exhaustion from unbounded recursion
- Queries that never complete

Without this limit, circular references or malformed data in truly self-referential structures could cause the query engine to recurse indefinitely. The type-aware implementation ensures safety for genuinely recursive structures while allowing natural hierarchies (like items) to traverse freely.
