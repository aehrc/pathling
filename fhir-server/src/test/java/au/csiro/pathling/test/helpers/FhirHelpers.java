/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.helpers;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import au.csiro.pathling.fhirpath.ResourceDefinition;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.function.memberof.ValidateCodeResult;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.parser.IParser;
import java.util.*;
import javax.annotation.Nonnull;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.BundleEntryResponseComponent;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Parameters;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * @author John Grimes
 */
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
   * to a $validate-code request using a Coding.
   */
  public static class ValidateCodingTxAnswerer implements Answer<Bundle> {

    private final Collection<Coding> validCodings = new HashSet<>();

    public ValidateCodingTxAnswerer(@Nonnull final Coding... validCodings) {
      this.validCodings.addAll(Arrays.asList(validCodings));
    }

    @Override
    @Nonnull
    public Bundle answer(@Nonnull final InvocationOnMock invocation) {
      final Bundle request = invocation.getArgument(0);
      final Bundle response = new Bundle();
      response.setType(BundleType.BATCHRESPONSE);

      // For each entry in the request, check whether it matches one of the valid concepts and add
      // an entry to the response bundle.
      for (final BundleEntryComponent entry : request.getEntry()) {
        final BundleEntryComponent responseEntry = new BundleEntryComponent();
        final BundleEntryResponseComponent responseEntryResponse = new BundleEntryResponseComponent();
        responseEntryResponse.setStatus("200");
        responseEntry.setResponse(responseEntryResponse);
        final Parameters responseParams = new Parameters();
        responseEntry.setResource(responseParams);

        final Coding codingParam = (Coding) ((Parameters) entry.getResource())
            .getParameter("coding");
        if (codingParam != null) {
          final boolean result = validCodings.stream()
              .anyMatch(validCoding -> codingsAreEqual(codingParam, validCoding));
          responseParams.setParameter("result", result);
        } else {
          responseParams.setParameter("result", false);
        }

        response.addEntry(responseEntry);
      }

      return response;
    }

  }

  /**
   * Custom Mockito answerer for returning a mock response from the terminology server in response
   * to a $validate-code request using a CodeableConcept.
   */
  public static class ValidateCodeableConceptTxAnswerer implements Answer<Bundle> {

    private final Collection<CodeableConcept> validCodeableConcepts = new HashSet<>();

    public ValidateCodeableConceptTxAnswerer(
        @Nonnull final CodeableConcept... validCodeableConcepts) {
      this.validCodeableConcepts.addAll(Arrays.asList(validCodeableConcepts));
    }

    @Override
    @Nonnull
    public Bundle answer(@Nonnull final InvocationOnMock invocation) {
      final Bundle request = invocation.getArgument(0);
      final Bundle response = new Bundle();
      response.setType(BundleType.BATCHRESPONSE);

      // For each entry in the request, check whether it matches one of the valid concepts and add
      // an entry to the response bundle.
      for (final BundleEntryComponent entry : request.getEntry()) {
        final BundleEntryComponent responseEntry = new BundleEntryComponent();
        final BundleEntryResponseComponent responseEntryResponse = new BundleEntryResponseComponent();
        responseEntryResponse.setStatus("200");
        responseEntry.setResponse(responseEntryResponse);
        final Parameters responseParams = new Parameters();
        responseEntry.setResource(responseParams);

        final CodeableConcept codeableConceptParam = (CodeableConcept) ((Parameters) entry
            .getResource())
            .getParameter("codeableConcept");
        if (codeableConceptParam != null) {
          final boolean result = validCodeableConcepts.stream()
              .anyMatch(validCodeableConcept -> codeableConceptsAreEqual(codeableConceptParam,
                  validCodeableConcept));
          responseParams.setParameter("result", result);
        } else {
          responseParams.setParameter("result", false);
        }

        response.addEntry(responseEntry);
      }

      return response;
    }

  }

  /**
   * Custom Mockito answerer for returning $validate-code results based on the correlation
   * identifiers in the input Rows.
   */
  public static class ValidateCodeMapperAnswerer implements Answer<Iterator<ValidateCodeResult>> {

    private final List<Boolean> expectedResults;

    public ValidateCodeMapperAnswerer(@Nonnull final Boolean... expectedResults) {
      this.expectedResults = Arrays.asList(expectedResults);
    }

    @Override
    @Nonnull
    public Iterator<ValidateCodeResult> answer(@Nonnull final InvocationOnMock invocation) {
      final List rows = IteratorUtils.toList(invocation.getArgument(0));
      final Collection<ValidateCodeResult> results = new ArrayList<>();

      for (int i = 0; i < rows.size(); i++) {
        final Row row = (Row) rows.get(i);
        final int hash = row.getInt(0);
        final boolean resultValue = expectedResults.get(i);
        final ValidateCodeResult result = new ValidateCodeResult(hash, resultValue);
        results.add(result);
      }

      return results.iterator();
    }

  }

}
