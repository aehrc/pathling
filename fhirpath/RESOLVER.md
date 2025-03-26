# Pathling FHIR Data Resolution Model

This document describes the data model and schema used by the `JoinResolver` class to represent FHIR resources in Spark datasets, including how joins between resources are handled.

## 1. Core Data Model

### 1.1 Resource Representation

Each FHIR resource type is represented as a Spark dataset with the following base structure:

- `key`: A unique identifier for the resource (typically the resource ID)
- `id`: Another representation of the resource ID (used for grouping operations)
- Resource content: The actual FHIR resource data as a struct column named after the resource type

### 1.2 Data Root Types

The system uses several types of data roots to represent different sources of data:

- **ResourceRoot**: Represents a direct FHIR resource (e.g., Patient)
- **JoinRoot**: Abstract base for joined resources
  - **ResolveRoot**: Represents resources referenced by the subject resource (forward references)
  - **ReverseResolveRoot**: Represents resources that reference the subject resource (backward references)

## 2. Column Naming Conventions

### 2.1 Resource Columns

- Subject resource columns are named after the resource type (e.g., `Patient`)
- The subject resource column contains a struct with all the resource's fields
- Foreign resources are represented as arrays of structs in columns named after the resource type

### 2.2 Join-Related Columns

For a join operation, several special columns are created:

- `{resourceType}__pkey`: Parent key column used during join operations
- `{resourceType}__ckey`: Child key column used during join operations
- `{resourceType}`: The column containing the joined resource data

### 2.3 Map Columns

Columns containing map data (typically for joined resources) follow these patterns:

- **Forward Resolve**: `id@{resourceType}` - Map where keys are referenced resource IDs and values are the referenced resources
- **Reverse Resolve**: `{childReferencePath}@{resourceType}` - Map where keys are master resource IDs and values are arrays of child resources

Map columns are identified by the presence of the `@` character. The part before `@` indicates the join type, and the part after indicates the resource type.

## 3. Join Operations

### 3.1 Resolve Join (Forward References)

When resolving references from a subject resource to referenced resources:

1. The reference path in the subject resource is evaluated to get reference keys
2. Referenced resources are retrieved and transformed into a map structure
3. The subject dataset is joined with the referenced resources using the reference keys
4. For singular references, a direct join is performed
5. For non-singular references, the subject dataset is expanded, joined, and then re-grouped

Example: `Patient.managingOrganization.resolve()` joins Patient resources with their referenced Organization resources. This creates a map column `id@Organization` containing the Organization resources.

### 3.2 Reverse Resolve Join (Backward References)

When resolving references from resources that reference the subject resource:

1. The reference path in the referencing resource is evaluated
2. Referencing resources are grouped by their reference to the subject resource
3. The subject dataset is joined with the grouped referencing resources
4. The result contains arrays of referencing resources for each subject resource

Example: `Patient.reverseResolve(Condition.subject)` joins Patient resources with Condition resources that reference them. This creates a map column `subject@Condition` containing arrays of Condition resources.

### 3.3 Foreign Resource Joins

When joining with resources that are neither the subject nor directly related:

1. The foreign resource dataset is retrieved
2. A cross join is performed with the subject dataset (with a performance warning)
3. The result contains all foreign resources for each subject resource

Foreign resources are represented as array columns named after the resource type (e.g., `Observation`).

## 4. Data Merging Rules

### 4.1 Map Column Merging

When datasets with overlapping map columns are joined:

- Map columns with the same name are merged using `SqlFunctions.ns_map_concat`
- This preserves data from both sources, with the right side taking precedence for duplicate keys
- The merge is performed using a temporary column that is later merged with the existing column

### 4.2 Non-Map Column Handling

For non-map columns during joins:

- Common columns from the left dataset are preserved
- Unique columns from the right dataset are added to the result
- This is handled in the `joinWithMapMerge` method

### 4.3 Collection Handling

For collections of resources (non-singular references):

- Resources are collected into arrays using `collect_list`
- Maps are aggregated using `SqlFunctions.collect_map` which handles null values and merges maps
- Other columns are aggregated using `any_value` to preserve a single value
- The dataset is grouped by the `id` column to ensure proper aggregation

## 5. Implementation Details

### 5.1 Dataset Transformations

The resolver uses several key transformations:

- `withMapMerge`: Creates a temporary column and merges it with an existing map column
- `joinWithMapMerge`: Joins two datasets while properly merging map columns
- `mapForeignColumns`: Maps operations across all foreign resource columns (those containing `@`)

### 5.2 Join Set Structure

Join operations are organized into a hierarchical structure using `JoinSet`:

- Each `JoinSet` has a master data root and children
- Children represent nested joins
- The structure forms a tree that represents the complete join operation

This hierarchical structure allows for complex, nested join operations to be represented and executed efficiently. The `JoinSet.mergeRoots` method combines multiple join operations into a single tree structure.

### 5.3 Performance Considerations

- Foreign resource joins use cross joins which can be inefficient for large datasets
- Non-singular references require expanding and re-grouping the dataset, which can be expensive
- Map columns are used to efficiently represent joined resources without duplicating data
- The `collect_map` function requires the Spark SQL configuration `spark.sql.mapKeyDedupPolicy=LAST_WIN`
