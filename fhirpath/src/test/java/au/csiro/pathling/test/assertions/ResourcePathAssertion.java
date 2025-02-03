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

package au.csiro.pathling.test.assertions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.execution.CollectionDataset;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * @author John Grimes
 */
public class ResourcePathAssertion extends BaseFhirPathAssertion<ResourcePathAssertion> {

  @Nonnull
  private final ResourceCollection fhirPath;

  ResourcePathAssertion(@Nonnull final ResourceCollection fhirPath,
      @Nonnull final CollectionDataset datasetResult) {
    super(datasetResult);
    this.fhirPath = fhirPath;
  }

  @Override
  @Nonnull
  protected Column getValueColumn() {
    return fhirPath.getColumnValue().getField("id");
  }
  
  @Nonnull
  public ResourcePathAssertion hasResourceType(@Nonnull final ResourceType type) {
    assertEquals(type, fhirPath.getResourceType());
    return this;
  }

}
