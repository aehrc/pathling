## MODIFIED Requirements

### Requirement: Parameter dropdown populated by resource type

The parameter name dropdown in each search parameter row SHALL display the
search parameters available for the currently selected resource type, as
extracted from the CapabilityStatement. Each option SHALL show the parameter
name, with the parameter type displayed as a badge next to it.

#### Scenario: Parameter list updates on resource type change

- **WHEN** the user changes the selected resource type from Patient to
  Observation
- **THEN** the parameter dropdown options SHALL update to show search parameters
  declared for Observation, and all search parameter rows SHALL be reset to a
  single empty row

#### Scenario: No search parameters available

- **WHEN** the selected resource type has no declared search parameters
- **THEN** the parameter dropdown SHALL be empty and display placeholder text
  indicating no parameters are available

#### Scenario: FHIRPath filters reset on resource type change

- **WHEN** the user has entered FHIRPath filter expressions and then changes the
  selected resource type
- **THEN** all FHIRPath filter rows SHALL be reset to a single empty row
