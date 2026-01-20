/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.execution;

import au.csiro.pathling.search.ResourceFilterFactory;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A minimal implementation of {@link BaseResourceResolver} that provides Column reference
 * generation without requiring a DataSource or SparkSession.
 * <p>
 * This resolver is designed for building filter expressions where only Column references are
 * needed, not actual data access. It enables {@link FhirPathEvaluator} to generate SparkSQL
 * Column expressions without requiring a connected data source.
 * <p>
 * Key characteristics:
 * <ul>
 *   <li>Does not require a DataSource or SparkSession</li>
 *   <li>{@link #createView()} throws {@link UnsupportedOperationException} since no data is
 *       available</li>
 *   <li>{@link #resolveSubjectResource()} creates Column references using
 *       {@code functions.col(resourceType.toCode())}</li>
 * </ul>
 * <p>
 * Use this resolver when building filter expressions that will be applied to datasets later,
 * rather than when executing queries against actual data.
 *
 * @see ResourceFilterFactory
 */
@EqualsAndHashCode(callSuper = true)
@Value
public class ColumnOnlyResolver extends BaseResourceResolver {

  /**
   * The resource type that this resolver provides Column references for.
   */
  @Nonnull
  ResourceType subjectResource;

  /**
   * The FHIR context used for resource definitions.
   */
  @Nonnull
  FhirContext fhirContext;
}
