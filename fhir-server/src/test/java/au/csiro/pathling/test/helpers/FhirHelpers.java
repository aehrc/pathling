/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.helpers;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import au.csiro.pathling.fhirpath.ResourceDefinition;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.function.memberof.MemberOfResult;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.parser.IParser;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.ValueSet;
import org.hl7.fhir.r4.model.ValueSet.ValueSetExpansionComponent;
import org.hl7.fhir.r4.model.ValueSet.ValueSetExpansionContainsComponent;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * @author John Grimes
 */
@SuppressWarnings("unused")
public class FhirHelpers {

  private static final FhirContext FHIR_CONTEXT = FhirContext.forR4();
  private static final IParser JSON_PARSER = FHIR_CONTEXT.newJsonParser();

  @Nonnull
  public static FhirContext getFhirContext() {
    return FHIR_CONTEXT;
  }

  @Nonnull
  public static IParser getJsonParser() {
    return JSON_PARSER;
  }

  @Nonnull
  public static Optional<ElementDefinition> getChildOfResource(@Nonnull final String resourceCode,
      @Nonnull final String elementName) {
    final RuntimeResourceDefinition hapiDefinition = getFhirContext()
        .getResourceDefinition(resourceCode);
    checkNotNull(hapiDefinition);
    final ResourceDefinition definition = new ResourceDefinition(
        ResourceType.fromCode(resourceCode), hapiDefinition);
    return definition.getChildElement(elementName);
  }

  private static boolean codingsAreEqual(@Nonnull final Coding coding1,
      @Nonnull final Coding coding2) {
    return coding1.getUserSelected() == coding2.getUserSelected() &&
        Objects.equals(coding1.getSystem(), coding2.getSystem()) &&
        Objects.equals(coding1.getVersion(), coding2.getVersion()) &&
        Objects.equals(coding1.getCode(), coding2.getCode()) &&
        Objects.equals(coding1.getDisplay(), coding2.getDisplay());
  }

  private static boolean codeableConceptsAreEqual(@Nonnull final CodeableConcept codeableConcept1,
      @Nonnull final CodeableConcept codeableConcept2) {
    final List<Coding> coding1 = codeableConcept1.getCoding();
    final List<Coding> coding2 = codeableConcept2.getCoding();
    checkNotNull(coding1);
    checkNotNull(coding2);

    final Iterator<Coding> iterator1 = coding1.iterator();
    final Iterator<Coding> iterator2 = coding2.iterator();
    while (iterator1.hasNext()) {
      final Coding next1 = iterator1.next();
      final Coding next2 = iterator2.next();
      if (next1 == null || next2 == null || !codingsAreEqual(next1, next2)) {
        return false;
      }
    }
    return Objects.equals(codeableConcept1.getText(), codeableConcept2.getText());
  }

  /**
   * Custom Mockito answerer for returning a mock response from the terminology server in response
   * to calculating a ValueSet intersection using {@code $expand}.
   */
  public static class MemberOfTxAnswerer implements Answer<ValueSet> {

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
    public ValueSet answer(@Nonnull final InvocationOnMock invocation) {
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

  /**
   * Custom Mockito answerer for returning @{link MemberOfResult} objects based on the correlation
   * identifiers in the input Rows.
   */
  public static class MemberOfMapperAnswerer implements Answer<Iterator<MemberOfResult>>,
      Serializable {

    private static final long serialVersionUID = -4277469595727802064L;
    private final List<Boolean> expectedResults;

    public MemberOfMapperAnswerer(@Nonnull final Boolean... expectedResults) {
      this.expectedResults = Arrays.asList(expectedResults);
    }

    @Override
    @Nonnull
    public Iterator<MemberOfResult> answer(@Nonnull final InvocationOnMock invocation) {
      final List rows = IteratorUtils.toList(invocation.getArgument(0));
      final Collection<MemberOfResult> results = new ArrayList<>();

      for (int i = 0; i < rows.size(); i++) {
        final Row row = (Row) rows.get(i);
        final int hash = row.getInt(0);
        final boolean resultValue = expectedResults.get(i);
        final MemberOfResult result = new MemberOfResult(hash, resultValue);
        results.add(result);
      }

      return results.iterator();
    }

  }

}
