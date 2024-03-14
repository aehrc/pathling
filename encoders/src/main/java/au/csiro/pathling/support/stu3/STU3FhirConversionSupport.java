/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2024 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.support.stu3;

import au.csiro.pathling.support.FhirConversionSupport;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.hl7.fhir.dstu3.model.Base;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.dstu3.model.Reference;
import org.hl7.fhir.dstu3.model.Resource;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * {@link FhirConversionSupport} implementation for FHIR STU3.
 */
public class STU3FhirConversionSupport extends FhirConversionSupport {

  private static final long serialVersionUID = -3851536580799176725L;

  @Override
  public String fhirType(final IBase base) {
    return base.fhirType();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Nonnull
  public <T extends IBaseResource> List<IBaseResource> extractEntryFromBundle(
      @Nonnull final IBaseBundle bundle,
      @Nonnull final Class<T> resourceClass) {
    final Bundle stu3Bundle = (Bundle) bundle;

    return stu3Bundle.getEntry().stream()
        .map(BundleEntryComponent::getResource)
        .filter(resourceClass::isInstance)
        .collect(Collectors.toList());
  }

  private static boolean isURNReference(@Nonnull final Reference reference) {
    return reference.hasReference() && reference.getReference().startsWith("urn:");
  }

  static void resolveURNReference(@Nonnull final Base base) {
    if (base instanceof Reference) {
      final Reference reference = (Reference) base;
      final Resource resource = (Resource) reference.getResource();
      if (isURNReference(reference) && resource != null && resource.hasIdElement()) {
        reference.setReference(resource.getIdElement().getValue());
      }
    }
  }

  private static void resolveURNReferences(@Nonnull final Resource resource) {
    STU3Traversal.processRecursive(resource, STU3FhirConversionSupport::resolveURNReference);
  }

  /**
   * {@inheritDoc}
   */
  @Nonnull
  @Override
  public IBaseBundle resolveReferences(@Nonnull final IBaseBundle bundle) {
    final Bundle stu3Bundle = (Bundle) bundle;
    stu3Bundle.getEntry().stream()
        .map(BundleEntryComponent::getResource)
        .forEach(STU3FhirConversionSupport::resolveURNReferences);
    return stu3Bundle;
  }

}
