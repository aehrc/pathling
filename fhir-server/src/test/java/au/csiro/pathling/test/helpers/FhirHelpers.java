/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.helpers;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import au.csiro.pathling.fhirpath.ResourceDefinition;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.r4.model.Base;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.springframework.stereotype.Component;

/**
 * @author John Grimes
 */
@Component
public class FhirHelpers {

  @Nonnull
  public static Optional<ElementDefinition> getChildOfResource(
      @Nonnull final FhirContext fhirContext, @Nonnull final String resourceCode,
      @Nonnull final String elementName) {
    final RuntimeResourceDefinition hapiDefinition = fhirContext
        .getResourceDefinition(resourceCode);
    checkNotNull(hapiDefinition);
    final ResourceDefinition definition = new ResourceDefinition(
        ResourceType.fromCode(resourceCode), hapiDefinition);
    return definition.getChildElement(elementName);
  }


  public static <T extends Base> T deepEq(@Nonnull final T expected) {
    return Mockito.argThat(new FhirDeepMatcher<>(expected));
  }

  private static class FhirDeepMatcher<T extends Base> implements ArgumentMatcher<T> {

    @Nonnull
    private final T expected;

    private FhirDeepMatcher(@Nonnull final T expected) {
      this.expected = expected;
    }

    @Override
    public boolean matches(@Nullable final T actual) {
      return expected.equalsDeep(actual);
    }
  }
}
