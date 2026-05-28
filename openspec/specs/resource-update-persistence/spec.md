# resource-update-persistence

## Purpose

Defines how the server persists resource updates to Delta tables, including
batch updates of same-type resources and recovery when a target table path
already exists.

## Requirements

### Requirement: Same-type batch updates persist in a single operation

The server SHALL accept a FHIR batch bundle containing multiple update (`PUT`)
entries for resources of the same type, group those resources by type, and
persist all resources of each type in a single merge into that type's Delta
table.

#### Scenario: Batch updates two patients

- **WHEN** a client POSTs a batch bundle with two `PUT` entries, one for
  `Patient/batch-patient-1` and one for `Patient/batch-patient-2`
- **THEN** the response status is `200 OK`
- **AND** the response is a `batch-response` bundle with two entries, each
  reporting status `200`
- **AND** both patients are retrievable from the server afterwards

#### Scenario: Batch updates resources of mixed types

- **WHEN** a client POSTs a batch bundle with `PUT` entries for resources of
  different types
- **THEN** the response status is `200 OK`
- **AND** each entry reports status `200`
- **AND** each resource is persisted to its corresponding Delta table

### Requirement: Update writes recover when the target path already exists

When persisting resources of a given type, the server SHALL complete the write
even if a directory already exists at the target table path. The server SHALL
NOT return an error caused by writing to an existing path.

#### Scenario: Target Delta table already exists

- **WHEN** the server persists an updated resource whose type already has a
  Delta table on disk
- **THEN** the existing rows are preserved, the updated resource is merged in by
  `id`, and the operation succeeds

#### Scenario: Target path exists but is not a recognised Delta table

- **WHEN** the server persists a resource and a directory exists at the target
  table path that is not a usable Delta table
- **THEN** the server initialises or resolves the table and completes the write
  rather than failing with a path-already-exists error
