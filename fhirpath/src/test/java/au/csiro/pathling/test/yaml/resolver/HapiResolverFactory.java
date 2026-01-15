/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.test.yaml.resolver;

import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.fhirpath.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.fhirpath.definition.fhir.FhirResourceTag;
import au.csiro.pathling.fhirpath.execution.DefaultResourceResolver;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.function.Function;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Factory for creating resource resolvers from HAPI FHIR resources. This implementation handles the
 * conversion of HAPI FHIR resource objects into a format suitable for FHIRPath expression
 * evaluation.
 */
@Value(staticConstructor = "of")
public class HapiResolverFactory implements Function<RuntimeContext, ResourceResolver> {

  @Nonnull IBaseResource resource;

  @Override
  @Nonnull
  public ResourceResolver apply(final RuntimeContext rt) {
    final Dataset<Row> resourceDS =
        rt.getSpark()
            .createDataset(List.of(resource), rt.getFhirEncoders().of(resource.fhirType()))
            .toDF();

    return DefaultResourceResolver.of(
        FhirResourceTag.of(ResourceType.fromCode(resource.fhirType())),
        FhirDefinitionContext.of(rt.getFhirEncoders().getContext()),
        resourceDS);
  }
}
