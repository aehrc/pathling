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
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.ValueSet;
import org.hl7.fhir.r4.model.ValueSet.ValueSetExpansionComponent;
import org.hl7.fhir.r4.model.ValueSet.ValueSetExpansionContainsComponent;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
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

  /**
   * Custom Mockito answerer for returning a mock response from the terminology server in response
   * to calculating a ValueSet intersection using {@code $expand}.
   */
  public static class MemberOfTxAnswerer implements Answer<ValueSet>, Serializable {

    private static final long serialVersionUID = -8343684022079397168L;
    private final Collection<Coding> validMembers;

    public MemberOfTxAnswerer(@Nonnull final Coding... validMembers) {
      this.validMembers = new HashSet<>();
      this.validMembers.addAll(Arrays.asList(validMembers));
    }

    public MemberOfTxAnswerer(@Nonnull final CodeableConcept... validMembers) {
      final List<Coding> codings = Arrays.stream(validMembers)
          .flatMap(codeableConcept -> codeableConcept.getCoding().stream())
          .collect(Collectors.toList());
      this.validMembers = new HashSet<>();
      this.validMembers.addAll(codings);
    }

    @Override
    @Nonnull
    public ValueSet answer(@Nullable final InvocationOnMock invocation) {
      final ValueSet answer = new ValueSet();
      final ValueSetExpansionComponent expansion = new ValueSetExpansionComponent();
      final List<ValueSetExpansionContainsComponent> contains = validMembers.stream()
          .map(validCoding -> {
            final ValueSetExpansionContainsComponent code = new ValueSetExpansionContainsComponent();
            code.setSystem(validCoding.getSystem());
            code.setCode(validCoding.getCode());
            code.setVersion(validCoding.getVersion());
            code.setDisplay(validCoding.getDisplay());
            return code;
          })
          .collect(Collectors.toList());
      expansion.setContains(contains);
      answer.setExpansion(expansion);
      return answer;
    }

  }

}
