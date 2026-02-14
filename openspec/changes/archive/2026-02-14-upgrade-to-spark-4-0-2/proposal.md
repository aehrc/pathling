## Why

Apache Spark 4.0.2 is now available, and Pathling is currently on Spark 4.0.1.
Upgrading keeps the project current with the latest bug fixes, performance
improvements, and security patches. As a library that runs on user-managed
Spark clusters, staying aligned with the latest Spark release ensures
compatibility with the environments users are deploying to.

## What changes

- Bump the minimum Spark version from 4.0.1 to 4.0.2 across all modules.
- Update the Catalyst compatibility layer in the encoders module if constructor
  signatures have changed.
- Update Scala version if required by Spark 4.0.2's transitive dependencies.
- Update R library sparklyr configuration if needed.
- Update documentation version references.

## Capabilities

### New capabilities

None.

### Modified capabilities

None — this is a dependency upgrade with no changes to user-facing
requirements.

## Impact

- **All core modules** (`utilities`, `encoders`, `terminology`, `fhirpath`,
  `library-api`, `library-runtime`): POM version property changes and
  recompilation.
- **Encoders module**: Catalyst compatibility layer (`Catalyst.scala`) may need
  updates for any changed internal APIs.
- **Python library** (`lib/python`): Dependency version updates for PySpark.
- **R library** (`lib/R`): Sparklyr compatibility verification.
- **Server module**: Rebuild against updated `library-runtime` artifact.
- **Documentation**: Version references in site docs and Python library.
