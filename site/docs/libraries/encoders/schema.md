---
description: A scheme for representing FHIR resources within a Parquet schema.
---

# Parquet specification

This specification describes a scheme for representing FHIR resources within a 
[Parquet](https://parquet.apache.org/) schema.

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "
SHOULD NOT", "RECOMMENDED",  "MAY", and "OPTIONAL" in this document are to be
interpreted as described in [RFC 2119](https://www.ietf.org/rfc/rfc2119.txt).

## Version compatibility

The following versions of FHIR are supported by this specification:

- [4.0.1 (R4)](https://hl7.org/fhir/R4)

## Resource type support

All R4 resource types are supported by this specification, except the following:

- Parameters
- Task
- StructureDefinition
- StructureMap
- Bundle

## Configuration options

There are several options that determine the structure of the schema. Any two 
schemas that conform to this specification with the same configuration values 
SHALL be identical and compatible.

| Option                | Description                                                 |
|-----------------------|-------------------------------------------------------------|
| Maximum nesting level | The maximum supported depth of nested element data.         |
| Extensions enabled    | Whether extension content is included within the schema.    |
| Supported open types  | The list of types that are supported for open choice types. |

## Out of scope

The following features are not currently supported by this specification:

- Primitive extensions
- Profiled resources
- Contained resources

## Schema structure

All fields SHALL be encoded as `OPTIONAL`, unless otherwise specified. All 
groups SHALL be encoded as `REQUIRED`.

### Primitive elements

Each primitive element within a resource definition SHALL be represented as a
field within the schema with the same name (except where otherwise specified).
The data type SHALL be determined by the element type according to the following
table:

| FHIR type   | Parquet type  | Additional requirements                                                                          |
|-------------|---------------|--------------------------------------------------------------------------------------------------|
| boolean     | BOOLEAN       |                                                                                                  |
| canonical   | BINARY (UTF8) | Compliant with the [FHIR canonical format](https://hl7.org/fhir/R4/datatypes.html#canonical)     |
| code        | BINARY (UTF8) | Compliant with the [FHIR code format](https://hl7.org/fhir/R4/datatypes.html#code)               |
| dateTime    | BINARY (UTF8) | Compliant with the [FHIR dateTime format](https://hl7.org/fhir/R4/datatypes.html#dateTime)       |
| date        | BINARY (UTF8) | Compliant with the [FHIR date format](https://hl7.org/fhir/R4/datatypes.html#date)               |
| decimal     | DECIMAL(32,6) | See [Decimal type](#decimal-type)                                                                |
| id          | BINARY (UTF8) | See [ID type](#id-type)                                                                          |
| instant     | INT96         |                                                                                                  |
| integer     | INT32         | Compliant with the [FHIR integer format](https://hl7.org/fhir/R4/datatypes.html#integer)         |
| markdown    | BINARY (UTF8) | Compliant with the [FHIR markdown format](https://hl7.org/fhir/R4/datatypes.html#markdown)       |
| oid         | BINARY (UTF8) | Compliant with the [FHIR oid format](https://hl7.org/fhir/R4/datatypes.html#oid)                 |
| positiveInt | INT32         | Compliant with the [FHIR positiveInt format](https://hl7.org/fhir/R4/datatypes.html#positiveInt) |
| string      | BINARY (UTF8) | Compliant with the [FHIR string format](https://hl7.org/fhir/R4/datatypes.html#string)           |
| time        | BINARY (UTF8) | Compliant with the [FHIR time format](https://hl7.org/fhir/R4/datatypes.html#time)               |
| unsignedInt | INT32         | Compliant with the [FHIR unsignedInt format](https://hl7.org/fhir/R4/datatypes.html#unsignedInt) |
| uri         | BINARY (UTF8) | Compliant with the [FHIR uri format](https://hl7.org/fhir/R4/datatypes.html#uri)                 |
| url         | BINARY (UTF8) | Compliant with the [FHIR url format](https://hl7.org/fhir/R4/datatypes.html#url)                 |
| uuid        | BINARY (UTF8) | Compliant with the [FHIR uuid format](https://hl7.org/fhir/R4/datatypes.html#uuid)               |

### Complex and backbone elements

Each complex and backbone element within a resource definition SHALL be
represented as a group within the schema with the same name. The group SHALL
contain a fields for each of the child elements.

Each field SHALL be represented as described in
the [Primitive elements](#primitive-elements) and [Choice types](#choice-types)
sections (or this section in the case of a nested complex or backbone element).

If the "extensions enabled" option is set to true, an additional field
SHALL be included with the name `_fid`. This field SHALL have a type of `INT32`.
This field SHALL contain a value that uniquely identifies the complex or 
backbone element within the resource.

### Choice types

If the choice type is open (i.e. a type of `*`), the group SHALL contain a
field for each type listed in the configuration value "supported open types".

If the choice type is not open, it SHALL be represented as a group with a
field for each of its valid types.

The name of each field SHALL follow the format `value[type]`, where `[type]` is
the name of the type in upper camel case.

### Decimal type

An element of type `decimal` SHALL be represented as a `DECIMAL` field with a
precision of 32 and a scale of 6.

In addition, an `INT32` field SHALL be included with the suffix `_scale`. This
field SHALL be used to store the scale of the decimal value from the original
FHIR data.

### ID type

An element of type `id` SHALL be represented as a `BINARY (UTF8)` field. This
field SHALL be used to store the FHIR resource logical ID.

In addition to this field, a `BINARY (UTF8)` field SHALL be included with the
suffix `_versioned`. This field SHALL be used to store a fully qualified logical
ID that includes the resource type and the technical version. The data in this
field SHALL follow the format `[resource type]/[logical ID]/_history/[version]`.

### Quantity type

If a complex element is of
type [Quantity](https://hl7.org/fhir/R4/datatypes.html#Quantity), an additional
two fields SHALL be included as part of the group. These fields SHALL be
named `_value_canonicalized` and `_code_canonicalized`.

The `_value_canonicalized` field SHALL be encoded as a group with the 
following fields:

- `value` with a type of decimal, precision 38 and scale 0.
- `scale` with a type of integer.

The `_code_canonicalized` field SHALL be encoded as a string field.

These fields MAY be used to store canonicalized versions of the `value`
and `code` fields from the original FHIR data, for easier comparison and
querying.

### Reference type

If a complex element is of
type [Reference](https://hl7.org/fhir/R4/references.html#Reference), it is 
represented as described in [Complex and backbone elements](#complex-and-backbone-elements) 
with the following exception:

* The `assigner` field within the `identifier` field within a `Reference` SHALL 
  NOT be included in the schema.

### Extensions

If the "extensions enabled" option is true, an additional field SHALL be
included at the root of the schema named `_extension`. This field SHALL have a
type of `MAP`, with an `INT32` key and a repeated group value.

The group used for each value in the map SHALL be represented using
the [Extension](https://hl7.org/fhir/R4/extensibility.html#extension) type, as
described in [Complex and backbone elements](#complex-and-backbone-elements).

If the "extensions enabled" option is true, this field SHALL be used to store
extension data from the original FHIR data (excluding primitive extensions).
Each key within the map SHALL contain a reference to the `_fid` of the element 
that the extension is attached to.

## Example

The following schema is an example of how to represent
the [Patient](https://hl7.org/fhir/R4/patient.html) resource type using the
following configuration values:

- Maximum nesting level: `3`
- Extensions enabled: `true`
- Supported open types: `boolean`, `code`, `date`, `dateTime`, `decimal`,
  `integer`, `string`, `Coding`, `CodeableConcept`, `Address`, `Identifier`,
  `Reference`

```
message spark_schema {
  optional binary id (STRING);
  optional binary id_versioned (STRING);
  optional group meta {
    optional binary id (STRING);
    optional binary versionId (STRING);
    optional binary versionId_versioned (STRING);
    optional int96 lastUpdated;
    optional binary source (STRING);
    optional group profile (LIST) {
      repeated group list {
        optional binary element (STRING);
      }
    }
    optional group security (LIST) {
      repeated group list {
        optional group element {
          optional binary id (STRING);
          optional binary system (STRING);
          optional binary version (STRING);
          optional binary code (STRING);
          optional binary display (STRING);
          optional boolean userSelected;
          optional int32 _fid;
        }
      }
    }
    optional group tag (LIST) {
      repeated group list {
        optional group element {
          optional binary id (STRING);
          optional binary system (STRING);
          optional binary version (STRING);
          optional binary code (STRING);
          optional binary display (STRING);
          optional boolean userSelected;
          optional int32 _fid;
        }
      }
    }
    optional int32 _fid;
  }
  optional binary implicitRules (STRING);
  optional binary language (STRING);
  optional group text {
    optional binary id (STRING);
    optional binary status (STRING);
    optional binary div (STRING);
    optional int32 _fid;
  }
  optional group identifier (LIST) {
    repeated group list {
      optional group element {
        optional binary id (STRING);
        optional binary use (STRING);
        optional group type {
          optional binary id (STRING);
          optional group coding (LIST) {
            repeated group list {
              optional group element {
                optional binary id (STRING);
                optional binary system (STRING);
                optional binary version (STRING);
                optional binary code (STRING);
                optional binary display (STRING);
                optional boolean userSelected;
                optional int32 _fid;
              }
            }
          }
          optional binary text (STRING);
          optional int32 _fid;
        }
        optional binary system (STRING);
        optional binary value (STRING);
        optional group period {
          optional binary id (STRING);
          optional binary start (STRING);
          optional binary end (STRING);
          optional int32 _fid;
        }
        optional group assigner {
          optional binary id (STRING);
          optional binary reference (STRING);
          optional binary type (STRING);
          optional group identifier {
            optional binary id (STRING);
            optional binary use (STRING);
            optional group type {
              optional binary id (STRING);
              optional group coding (LIST) {
                repeated group list {
                  optional group element {
                    optional binary id (STRING);
                    optional binary system (STRING);
                    optional binary version (STRING);
                    optional binary code (STRING);
                    optional binary display (STRING);
                    optional boolean userSelected;
                    optional int32 _fid;
                  }
                }
              }
              optional binary text (STRING);
              optional int32 _fid;
            }
            optional binary system (STRING);
            optional binary value (STRING);
            optional group period {
              optional binary id (STRING);
              optional binary start (STRING);
              optional binary end (STRING);
              optional int32 _fid;
            }
            optional int32 _fid;
          }
          optional binary display (STRING);
          optional int32 _fid;
        }
        optional int32 _fid;
      }
    }
  }
  optional boolean active;
  optional group name (LIST) {
    repeated group list {
      optional group element {
        optional binary id (STRING);
        optional binary use (STRING);
        optional binary text (STRING);
        optional binary family (STRING);
        optional group given (LIST) {
          repeated group list {
            optional binary element (STRING);
          }
        }
        optional group prefix (LIST) {
          repeated group list {
            optional binary element (STRING);
          }
        }
        optional group suffix (LIST) {
          repeated group list {
            optional binary element (STRING);
          }
        }
        optional group period {
          optional binary id (STRING);
          optional binary start (STRING);
          optional binary end (STRING);
          optional int32 _fid;
        }
        optional int32 _fid;
      }
    }
  }
  optional group telecom (LIST) {
    repeated group list {
      optional group element {
        optional binary id (STRING);
        optional binary system (STRING);
        optional binary value (STRING);
        optional binary use (STRING);
        optional int32 rank;
        optional group period {
          optional binary id (STRING);
          optional binary start (STRING);
          optional binary end (STRING);
          optional int32 _fid;
        }
        optional int32 _fid;
      }
    }
  }
  optional binary gender (STRING);
  optional binary birthDate (STRING);
  optional boolean deceasedBoolean;
  optional binary deceasedDateTime (STRING);
  optional group address (LIST) {
    repeated group list {
      optional group element {
        optional binary id (STRING);
        optional binary use (STRING);
        optional binary type (STRING);
        optional binary text (STRING);
        optional group line (LIST) {
          repeated group list {
            optional binary element (STRING);
          }
        }
        optional binary city (STRING);
        optional binary district (STRING);
        optional binary state (STRING);
        optional binary postalCode (STRING);
        optional binary country (STRING);
        optional group period {
          optional binary id (STRING);
          optional binary start (STRING);
          optional binary end (STRING);
          optional int32 _fid;
        }
        optional int32 _fid;
      }
    }
  }
  optional group maritalStatus {
    optional binary id (STRING);
    optional group coding (LIST) {
      repeated group list {
        optional group element {
          optional binary id (STRING);
          optional binary system (STRING);
          optional binary version (STRING);
          optional binary code (STRING);
          optional binary display (STRING);
          optional boolean userSelected;
          optional int32 _fid;
        }
      }
    }
    optional binary text (STRING);
    optional int32 _fid;
  }
  optional boolean multipleBirthBoolean;
  optional int32 multipleBirthInteger;
  optional group photo (LIST) {
    repeated group list {
      optional group element {
        optional binary id (STRING);
        optional binary contentType (STRING);
        optional binary language (STRING);
        optional binary data;
        optional binary url (STRING);
        optional int32 size;
        optional binary hash;
        optional binary title (STRING);
        optional binary creation (STRING);
        optional int32 _fid;
      }
    }
  }
  optional group contact (LIST) {
    repeated group list {
      optional group element {
        optional binary id (STRING);
        optional group relationship (LIST) {
          repeated group list {
            optional group element {
              optional binary id (STRING);
              optional group coding (LIST) {
                repeated group list {
                  optional group element {
                    optional binary id (STRING);
                    optional binary system (STRING);
                    optional binary version (STRING);
                    optional binary code (STRING);
                    optional binary display (STRING);
                    optional boolean userSelected;
                    optional int32 _fid;
                  }
                }
              }
              optional binary text (STRING);
              optional int32 _fid;
            }
          }
        }
        optional group name {
          optional binary id (STRING);
          optional binary use (STRING);
          optional binary text (STRING);
          optional binary family (STRING);
          optional group given (LIST) {
            repeated group list {
              optional binary element (STRING);
            }
          }
          optional group prefix (LIST) {
            repeated group list {
              optional binary element (STRING);
            }
          }
          optional group suffix (LIST) {
            repeated group list {
              optional binary element (STRING);
            }
          }
          optional group period {
            optional binary id (STRING);
            optional binary start (STRING);
            optional binary end (STRING);
            optional int32 _fid;
          }
          optional int32 _fid;
        }
        optional group telecom (LIST) {
          repeated group list {
            optional group element {
              optional binary id (STRING);
              optional binary system (STRING);
              optional binary value (STRING);
              optional binary use (STRING);
              optional int32 rank;
              optional group period {
                optional binary id (STRING);
                optional binary start (STRING);
                optional binary end (STRING);
                optional int32 _fid;
              }
              optional int32 _fid;
            }
          }
        }
        optional group address {
          optional binary id (STRING);
          optional binary use (STRING);
          optional binary type (STRING);
          optional binary text (STRING);
          optional group line (LIST) {
            repeated group list {
              optional binary element (STRING);
            }
          }
          optional binary city (STRING);
          optional binary district (STRING);
          optional binary state (STRING);
          optional binary postalCode (STRING);
          optional binary country (STRING);
          optional group period {
            optional binary id (STRING);
            optional binary start (STRING);
            optional binary end (STRING);
            optional int32 _fid;
          }
          optional int32 _fid;
        }
        optional binary gender (STRING);
        optional group organization {
          optional binary id (STRING);
          optional binary reference (STRING);
          optional binary type (STRING);
          optional group identifier {
            optional binary id (STRING);
            optional binary use (STRING);
            optional group type {
              optional binary id (STRING);
              optional group coding (LIST) {
                repeated group list {
                  optional group element {
                    optional binary id (STRING);
                    optional binary system (STRING);
                    optional binary version (STRING);
                    optional binary code (STRING);
                    optional binary display (STRING);
                    optional boolean userSelected;
                    optional int32 _fid;
                  }
                }
              }
              optional binary text (STRING);
              optional int32 _fid;
            }
            optional binary system (STRING);
            optional binary value (STRING);
            optional group period {
              optional binary id (STRING);
              optional binary start (STRING);
              optional binary end (STRING);
              optional int32 _fid;
            }
            optional int32 _fid;
          }
          optional binary display (STRING);
          optional int32 _fid;
        }
        optional group period {
          optional binary id (STRING);
          optional binary start (STRING);
          optional binary end (STRING);
          optional int32 _fid;
        }
        optional int32 _fid;
      }
    }
  }
  optional group communication (LIST) {
    repeated group list {
      optional group element {
        optional binary id (STRING);
        optional group language {
          optional binary id (STRING);
          optional group coding (LIST) {
            repeated group list {
              optional group element {
                optional binary id (STRING);
                optional binary system (STRING);
                optional binary version (STRING);
                optional binary code (STRING);
                optional binary display (STRING);
                optional boolean userSelected;
                optional int32 _fid;
              }
            }
          }
          optional binary text (STRING);
          optional int32 _fid;
        }
        optional boolean preferred;
        optional int32 _fid;
      }
    }
  }
  optional group generalPractitioner (LIST) {
    repeated group list {
      optional group element {
        optional binary id (STRING);
        optional binary reference (STRING);
        optional binary type (STRING);
        optional group identifier {
          optional binary id (STRING);
          optional binary use (STRING);
          optional group type {
            optional binary id (STRING);
            optional group coding (LIST) {
              repeated group list {
                optional group element {
                  optional binary id (STRING);
                  optional binary system (STRING);
                  optional binary version (STRING);
                  optional binary code (STRING);
                  optional binary display (STRING);
                  optional boolean userSelected;
                  optional int32 _fid;
                }
              }
            }
            optional binary text (STRING);
            optional int32 _fid;
          }
          optional binary system (STRING);
          optional binary value (STRING);
          optional group period {
            optional binary id (STRING);
            optional binary start (STRING);
            optional binary end (STRING);
            optional int32 _fid;
          }
          optional int32 _fid;
        }
        optional binary display (STRING);
        optional int32 _fid;
      }
    }
  }
  optional group managingOrganization {
    optional binary id (STRING);
    optional binary reference (STRING);
    optional binary type (STRING);
    optional group identifier {
      optional binary id (STRING);
      optional binary use (STRING);
      optional group type {
        optional binary id (STRING);
        optional group coding (LIST) {
          repeated group list {
            optional group element {
              optional binary id (STRING);
              optional binary system (STRING);
              optional binary version (STRING);
              optional binary code (STRING);
              optional binary display (STRING);
              optional boolean userSelected;
              optional int32 _fid;
            }
          }
        }
        optional binary text (STRING);
        optional int32 _fid;
      }
      optional binary system (STRING);
      optional binary value (STRING);
      optional group period {
        optional binary id (STRING);
        optional binary start (STRING);
        optional binary end (STRING);
        optional int32 _fid;
      }
      optional int32 _fid;
    }
    optional binary display (STRING);
    optional int32 _fid;
  }
  optional group link (LIST) {
    repeated group list {
      optional group element {
        optional binary id (STRING);
        optional group other {
          optional binary id (STRING);
          optional binary reference (STRING);
          optional binary type (STRING);
          optional group identifier {
            optional binary id (STRING);
            optional binary use (STRING);
            optional group type {
              optional binary id (STRING);
              optional group coding (LIST) {
                repeated group list {
                  optional group element {
                    optional binary id (STRING);
                    optional binary system (STRING);
                    optional binary version (STRING);
                    optional binary code (STRING);
                    optional binary display (STRING);
                    optional boolean userSelected;
                    optional int32 _fid;
                  }
                }
              }
              optional binary text (STRING);
              optional int32 _fid;
            }
            optional binary system (STRING);
            optional binary value (STRING);
            optional group period {
              optional binary id (STRING);
              optional binary start (STRING);
              optional binary end (STRING);
              optional int32 _fid;
            }
            optional int32 _fid;
          }
          optional binary display (STRING);
          optional int32 _fid;
        }
        optional binary type (STRING);
        optional int32 _fid;
      }
    }
  }
  optional int32 _fid;
  optional group _extension (MAP) {
    repeated group key_value {
      required int32 key;
      required group value (LIST) {
        repeated group list {
          optional group element {
            optional binary id (STRING);
            optional binary url (STRING);
            optional group valueAddress {
              optional binary id (STRING);
              optional binary use (STRING);
              optional binary type (STRING);
              optional binary text (STRING);
              optional group line (LIST) {
                repeated group list {
                  optional binary element (STRING);
                }
              }
              optional binary city (STRING);
              optional binary district (STRING);
              optional binary state (STRING);
              optional binary postalCode (STRING);
              optional binary country (STRING);
              optional group period {
                optional binary id (STRING);
                optional binary start (STRING);
                optional binary end (STRING);
                optional int32 _fid;
              }
              optional int32 _fid;
            }
            optional boolean valueBoolean;
            optional binary valueCode (STRING);
            optional group valueCodeableConcept {
              optional binary id (STRING);
              optional group coding (LIST) {
                repeated group list {
                  optional group element {
                    optional binary id (STRING);
                    optional binary system (STRING);
                    optional binary version (STRING);
                    optional binary code (STRING);
                    optional binary display (STRING);
                    optional boolean userSelected;
                    optional int32 _fid;
                  }
                }
              }
              optional binary text (STRING);
              optional int32 _fid;
            }
            optional group valueCoding {
              optional binary id (STRING);
              optional binary system (STRING);
              optional binary version (STRING);
              optional binary code (STRING);
              optional binary display (STRING);
              optional boolean userSelected;
              optional int32 _fid;
            }
            optional binary valueDateTime (STRING);
            optional binary valueDate (STRING);
            optional fixed_len_byte_array(14) valueDecimal (DECIMAL(32,6));
            optional int32 valueDecimal_scale;
            optional group valueIdentifier {
              optional binary id (STRING);
              optional binary use (STRING);
              optional group type {
                optional binary id (STRING);
                optional group coding (LIST) {
                  repeated group list {
                    optional group element {
                      optional binary id (STRING);
                      optional binary system (STRING);
                      optional binary version (STRING);
                      optional binary code (STRING);
                      optional binary display (STRING);
                      optional boolean userSelected;
                      optional int32 _fid;
                    }
                  }
                }
                optional binary text (STRING);
                optional int32 _fid;
              }
              optional binary system (STRING);
              optional binary value (STRING);
              optional group period {
                optional binary id (STRING);
                optional binary start (STRING);
                optional binary end (STRING);
                optional int32 _fid;
              }
              optional group assigner {
                optional binary id (STRING);
                optional binary reference (STRING);
                optional binary type (STRING);
                optional group identifier {
                  optional binary id (STRING);
                  optional binary use (STRING);
                  optional group type {
                    optional binary id (STRING);
                    optional group coding (LIST) {
                      repeated group list {
                        optional group element {
                          optional binary id (STRING);
                          optional binary system (STRING);
                          optional binary version (STRING);
                          optional binary code (STRING);
                          optional binary display (STRING);
                          optional boolean userSelected;
                          optional int32 _fid;
                        }
                      }
                    }
                    optional binary text (STRING);
                    optional int32 _fid;
                  }
                  optional binary system (STRING);
                  optional binary value (STRING);
                  optional group period {
                    optional binary id (STRING);
                    optional binary start (STRING);
                    optional binary end (STRING);
                    optional int32 _fid;
                  }
                  optional int32 _fid;
                }
                optional binary display (STRING);
                optional int32 _fid;
              }
              optional int32 _fid;
            }
            optional int32 valueInteger;
            optional group valueReference {
              optional binary id (STRING);
              optional binary reference (STRING);
              optional binary type (STRING);
              optional group identifier {
                optional binary id (STRING);
                optional binary use (STRING);
                optional group type {
                  optional binary id (STRING);
                  optional group coding (LIST) {
                    repeated group list {
                      optional group element {
                        optional binary id (STRING);
                        optional binary system (STRING);
                        optional binary version (STRING);
                        optional binary code (STRING);
                        optional binary display (STRING);
                        optional boolean userSelected;
                        optional int32 _fid;
                      }
                    }
                  }
                  optional binary text (STRING);
                  optional int32 _fid;
                }
                optional binary system (STRING);
                optional binary value (STRING);
                optional group period {
                  optional binary id (STRING);
                  optional binary start (STRING);
                  optional binary end (STRING);
                  optional int32 _fid;
                }
                optional int32 _fid;
              }
              optional binary display (STRING);
              optional int32 _fid;
            }
            optional binary valueString (STRING);
            optional int32 _fid;
          }
        }
      }
    }
  }
}
```
