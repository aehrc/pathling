## 1. Update version properties

- [x] 1.1 Update `pathling.sparkVersion` from `4.0.1` to `4.0.2` in root `pom.xml`
- [x] 1.2 Update `pathling.sparkVersion` from `4.0.1` to `4.0.2` in `server/pom.xml`
- [x] 1.3 Update `pathling.Rapi.sparkVersion` from `4.0.1` to `4.0.2` in `lib/R/pom.xml`

## 2. Verify Catalyst compatibility

- [x] 2.1 Check whether `StaticInvoke` constructor signature has changed in Spark 4.0.2 and update `Catalyst.scala` if needed

## 3. Build and test core modules

- [x] 3.1 Build core libraries with `mvn clean install -pl library-runtime -am`
- [x] 3.2 Fix any compilation errors in `encoders` (Scala code, Catalyst API)
- [x] 3.3 Fix any compilation errors in remaining core modules
- [x] 3.4 Run core module test suite and fix any failures

## 4. Build and test server

- [x] 4.1 Rebuild `library-runtime` and then build server with `mvn clean verify` from `server/`
- [x] 4.2 Fix any server test failures

## 5. Build and test language libraries

- [x] 5.1 Build and test Python library (`mvn clean install -pl lib/python -am`)
- [x] 5.2 Build and test R library (`mvn clean install -pl lib/R -am`)

## 6. Update documentation

- [x] 6.1 Update `site/docs/libraries/installation/spark.md` — update R example `version = "4.0.1"` to `"4.0.2"`
- [x] 6.2 Update `lib/python/LICENSE` — update Spark dependency version strings from `4.0.1` to `4.0.2`
