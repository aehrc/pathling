/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.security;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.sink.DataSinkBuilder;
import au.csiro.pathling.library.io.source.DataSourceBuilder;
import au.csiro.pathling.library.io.source.DatasetSource;
import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.UUID;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.util.TestDataSetup;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;


/**
 * @see <a
 * href="https://docs.spring.io/spring-security/site/docs/5.2.x/reference/html/test.html">Spring
 * Security - Testing</a>
 * @see <a
 * href="https://stackoverflow.com/questions/58289509/in-spring-boot-test-how-do-i-map-a-temporary-folder-to-a-configuration-property">In
 * Spring Boot Test, how do I map a temporary folder to a configuration property?</a>
 */
@Slf4j
@ActiveProfiles({"core", "unit-test"})
// @DirtiesContext(classMode = ClassMode.AFTER_CLASS)
abstract class SecurityTestForResources extends SecurityTest {

  @Autowired
  SparkSession spark;

  @TempDir
  @SuppressWarnings({"unused", "WeakerAccess"})
  static Path testRootDir;
  
  static Path dataDir;
  
  @Autowired
  private PathlingContext pathlingContext;
  
  protected static void registerPropertiesFromChildClasses(final DynamicPropertyRegistry registry, UUID uuid) {
    log.error("REGISTERING: " + uuid.toString());
    dataDir = testRootDir.resolve(uuid.toString());
    TestDataSetup.staticCopyTestDataToTempDir(dataDir);
    registry.add("pathling.storage.warehouseUrl",
        () -> "file://" + dataDir.toString().replaceFirst("/$", ""));
  }

  @AfterEach
  void tearDown() {
    spark.sqlContext().clearCache();
  }

  void assertWriteSuccess() {
    //new DataSinkBuilder(pathlingContext, dataSource).saveMode("overwrite").delta(dataDir.toString());
  }

  void assertUpdateSuccess() {
    //new DataSinkBuilder(pathlingContext, dataSource).saveMode("merge").delta(dataDir.toString());
  }

  void assertReadSuccess() {
    new DataSourceBuilder(pathlingContext).delta(dataDir.toString());
    
    
    //assertTrue(dataSource.getResourceTypes().contains(ResourceType.PATIENT.toCode()), "Checking security here, patient data should always exist.");
    //dataSource.read(ResourceType.PATIENT.toCode());
  }
}
