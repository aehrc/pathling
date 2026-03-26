## ADDED Requirements

### Requirement: Select function evaluates projection for each element

The FHIRPath engine SHALL support the `select(projection: expression) : collection` function. The function SHALL evaluate the projection expression for each item in the input collection. The result of each evaluation SHALL be added to the output collection.

#### Scenario: Simple property projection

- **WHEN** `select` is called with a property path expression on a collection of resources (e.g., `Patient.name.select(family)`)
- **THEN** the result SHALL contain the values of that property for each element in the input collection

#### Scenario: Expression projection

- **WHEN** `select` is called with a complex expression (e.g., `Patient.name.select(given + ' ' + family)`)
- **THEN** the result SHALL contain the evaluated expression result for each element

### Requirement: Select function flattens results

If the evaluation of the projection expression for an element results in a collection with multiple items, all items SHALL be added to the output collection (collections resulting from evaluation of the projection are flattened).

#### Scenario: Projection returns multiple items

- **WHEN** `select` is called with an expression that returns multiple values per element (e.g., `Patient.name.select(given)` where a name has multiple given names)
- **THEN** all values from each element's projection SHALL be included in the output collection

### Requirement: Select function handles empty results

If the evaluation of the projection expression for an element results in the empty collection, no element SHALL be added to the result for that input element.

#### Scenario: Projection returns empty for some elements

- **WHEN** `select` is called and the projection expression evaluates to empty for some input elements
- **THEN** those elements SHALL not contribute to the output collection

### Requirement: Select function handles empty input

If the input collection is empty, the result SHALL be empty.

#### Scenario: Empty input collection

- **WHEN** `select` is called on an empty collection
- **THEN** the result SHALL be an empty collection

### Requirement: Select function is available as a FHIRPath function

The `select` function SHALL be registered and invocable using standard FHIRPath function call syntax (e.g., `collection.select(expression)`).

#### Scenario: Function invocation syntax

- **WHEN** a FHIRPath expression uses `select` with dot notation (e.g., `Patient.contact.select(name)`)
- **THEN** the function SHALL be resolved and executed correctly
