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

package au.csiro.pathling.terminology;

import static au.csiro.pathling.test.TestResources.assertJson;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

abstract class MappingTest {

  IParser jsonParser;

  @Nullable
  TestInfo testInfo;

  @BeforeEach
  void setUp(@Nonnull final TestInfo testInfo) {
    jsonParser = FhirContext.forR4().newJsonParser();
    this.testInfo = testInfo;
  }

  void assertRequest(
      @Nonnull final IBaseResource resource) {
    //noinspection ConstantConditions,OptionalGetWithoutIsPresent
    assertRequest(resource, testInfo.getTestMethod().get().getName());
  }

  void assertRequest(
      @Nonnull final IBaseResource resource, @Nonnull final String name) {
    final String actualJson = jsonParser.encodeResourceToString(resource);

    //noinspection ConstantConditions,OptionalGetWithoutIsPresent
    final Path expectedPath = Paths
        .get("requests", testInfo.getTestClass().get().getSimpleName(),
            name + ".json");
    assertJson(expectedPath.toString(), actualJson);
  }

}
