package au.csiro.pathling.test.helpers;

import static au.csiro.pathling.test.helpers.FhirMatchers.codingEq;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.codingEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import au.csiro.pathling.terminology.TerminologyService2;
import au.csiro.pathling.terminology.TranslateMapping.TranslationEntry;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.ValueSet;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome.EQUIVALENT;
import static org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome.NOTSUBSUMED;
import static org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome.SUBSUMES;
import static org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome.SUBSUMEDBY;

public class TerminologyServiceHelpers {

  public final static Parameters RESULT_TRUE = new Parameters().setParameter("result", true);
  public final static Parameters RESULT_FALSE = new Parameters().setParameter("result", false);


  public final static Parameters OUTCOME_EQUIVALENT = new Parameters().setParameter("outcome",
      EQUIVALENT.toCode());

  public final static Parameters OUTCOME_SUBSUMES = new Parameters().setParameter("outcome",
      SUBSUMES.toCode());
  public final static Parameters OUTCOME_SUBSUMEDBY = new Parameters().setParameter("outcome",
      SUBSUMEDBY.toCode());


  public final static Parameters OUTCOME_NOTSUBSUMED = new Parameters().setParameter("outcome",
      NOTSUBSUMED.toCode());

  public static class ValidateExpectations {

    private final @Nonnull
    TerminologyService2 mockService;

    ValidateExpectations(@Nonnull final TerminologyService2 mockService) {
      this.mockService = mockService;
      clearInvocations(mockService);
      when(mockService.validate(any(), any())).thenReturn(RESULT_FALSE);
    }

    @Nonnull
    public ValidateExpectations withValueSet(@Nonnull final String valueSetUrl,
        @Nonnull final Coding... codings) {
      for (Coding coding : codings) {
        when(mockService.validate(eq(valueSetUrl), codingEq(coding))).thenReturn(RESULT_TRUE);
      }
      return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    @Nonnull
    public ValidateExpectations fromValueSet(@Nonnull final String valueSetUri,
        @Nonnull final ValueSet valueSet) {
      final Coding[] codings = valueSet.getExpansion().getContains().stream()
          .map(contains -> new Coding(contains.getSystem(), contains.getCode(),
              contains.getDisplay()))
          .toArray(Coding[]::new);
      return withValueSet(valueSetUri, codings);
    }
  }

  public static class TranslateExpectations {

    private final @Nonnull
    TerminologyService2 mockService;

    TranslateExpectations(@Nonnull final TerminologyService2 mockService) {
      this.mockService = mockService;
      clearInvocations(mockService);
      when(mockService.translate(any(), any(), anyBoolean())).thenReturn(RESULT_FALSE);
    }

    public TranslateExpectations withTranslations(final @Nonnull Coding coding,
        final @Nonnull String conceptMapUrl,
        final @Nonnull TranslationEntry... translations) {
      return withTranslations(coding, conceptMapUrl, false, translations);
    }


    public TranslateExpectations withMockTranslations(final @Nonnull Coding sourceCoding,
        final @Nonnull String conceptMapUrl, final @Nonnull String toSystem,
        final int noOfMappings) {

      final TranslationEntry[] translations = IntStream.range(0, noOfMappings)
          .mapToObj(i -> TerminologyHelpers.mockCoding(toSystem, sourceCoding.getCode(), i))
          .map(coding -> TranslationEntry.of(ConceptMapEquivalence.EQUIVALENT, coding))
          .toArray(TranslationEntry[]::new);

      return withTranslations(sourceCoding, conceptMapUrl, false, translations);
    }

    public TranslateExpectations withTranslations(
        final @Nonnull Coding coding,
        final @Nonnull String conceptMapUrl,
        final boolean reverse, final @Nonnull TranslationEntry... translations) {

      final Parameters translateResponse = new Parameters()
          .setParameter("result", true);

      for (TranslationEntry entry : translations) {
        final ParametersParameterComponent matchParameter1 = translateResponse.addParameter()
            .setName("match");
        matchParameter1.addPart().setName("equivalence").setValue(entry.getEquivalence());
        matchParameter1.addPart().setName("concept").setValue(entry.getConcept());
      }
      when(
          mockService.translate(codingEq(coding), eq(conceptMapUrl), eq(reverse))).thenReturn(
          translateResponse);
      return this;
    }
  }

  public static class SubsumesExpectations {

    private final @Nonnull
    TerminologyService2 mockService;

    private static class DefaultAnswer implements Answer<Parameters> {

      @Override
      public Parameters answer(final InvocationOnMock invocationOnMock) {
        final Coding codingA = invocationOnMock.getArgument(0);
        final Coding codingB = invocationOnMock.getArgument(1);

        if (codingA != null && codingEquals(codingA, codingB)) {
          return OUTCOME_EQUIVALENT;
        } else {
          return OUTCOME_NOTSUBSUMED;
        }
      }
    }

    public SubsumesExpectations(@Nonnull final TerminologyService2 mockService) {
      this.mockService = mockService;
      clearInvocations(mockService);
      doAnswer(new DefaultAnswer()).when(mockService).subsumes(any(), any());
    }

    public SubsumesExpectations withSubsumes(@Nonnull final Coding codingA,
        @Nonnull final Coding codingB) {
      when(mockService.subsumes(codingEq(codingA), codingEq(codingB))).thenReturn(OUTCOME_SUBSUMES);
      when(mockService.subsumes(codingEq(codingB), codingEq(codingA))).thenReturn(
          OUTCOME_SUBSUMEDBY);
      return this;
    }
  }


  public static ValidateExpectations setupValidate(final @Nonnull TerminologyService2 mockService) {
    return new ValidateExpectations(mockService);
  }


  public static TranslateExpectations setupTranslate(
      final @Nonnull TerminologyService2 mockService) {
    return new TranslateExpectations(mockService);
  }


  public static SubsumesExpectations setupSubsumes(
      final @Nonnull TerminologyService2 mockService) {
    return new SubsumesExpectations(mockService);
  }


}
