/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.io;

import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.config.Configuration;
import au.csiro.pathling.config.ImportConfiguration;
import au.csiro.pathling.errors.SecurityError;
import java.util.Arrays;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Test some basic Orderable behaviour across different FhirPath types.
 *
 * @author Piotr Szul
 */
@Tag("UnitTest")
class AccessRulesTest {

  final AccessRules accessRules;

  AccessRulesTest() {
    final Configuration configuration = new Configuration();
    final ImportConfiguration importConf = new ImportConfiguration();
    configuration.setImport(importConf);
    importConf.setAllowableSources(Arrays.asList("file:///usr/share/import", "s3://data/"));
    this.accessRules = new AccessRules(configuration);
  }

  @Test
  void allowsConfiguredSources() {
    accessRules.checkCanImportFrom("file:///usr/share/import");
    accessRules.checkCanImportFrom("file:///usr/share/import/somefile.json");
  }

  @Test
  void prohibitsAccessToUnlistedSources() {
    assertThrows(SecurityError.class,
        () -> accessRules.checkCanImportFrom("file:///usr/share/other"));
  }

  @Test
  void operatesOnPhysicalUrls() {
    accessRules.checkCanImportFrom("s3a://data/file.json");
    assertThrows(SecurityError.class, () -> accessRules.checkCanImportFrom("s3://data/file.json"));
  }

}
