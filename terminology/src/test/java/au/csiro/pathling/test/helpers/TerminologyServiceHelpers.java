package au.csiro.pathling.test.helpers;

import static au.csiro.pathling.test.helpers.FhirMatchers.codingEq;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.codingEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import au.csiro.pathling.terminology.TerminologyService2;
import au.csiro.pathling.terminology.TerminologyService2.Property;
import au.csiro.pathling.terminology.TerminologyService2.Translation;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.ValueSet;
import org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome;
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

  public static class ValidateExpectations {

    @Nonnull
    private final TerminologyService2 mockService;

    ValidateExpectations(@Nonnull final TerminologyService2 mockService) {
      this.mockService = mockService;
      clearInvocations(mockService);
      when(mockService.validateCode(any(), any())).thenReturn(false);
    }

    @Nonnull
    public ValidateExpectations withValueSet(@Nonnull final String valueSetUrl,
        @Nonnull final Coding... codings) {
      for (Coding coding : codings) {
        when(mockService.validateCode(eq(valueSetUrl), codingEq(coding))).thenReturn(true);
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

    @Nonnull
    private final TerminologyService2 mockService;

    TranslateExpectations(@Nonnull final TerminologyService2 mockService) {
      this.mockService = mockService;
      clearInvocations(mockService);
      when(mockService.translate(any(), any(), anyBoolean(), isNull())).thenReturn(
          Collections.emptyList());
    }

    public TranslateExpectations withTranslations(@Nonnull final Coding coding,
        @Nonnull final String conceptMapUrl,
        @Nonnull final Translation... translations) {
      return withTranslations(coding, conceptMapUrl, false, translations);
    }


    public TranslateExpectations withMockTranslations(@Nonnull final Coding sourceCoding,
        @Nonnull final String conceptMapUrl, @Nonnull final String toSystem,
        final int noOfMappings) {

      final Translation[] translations = IntStream.range(0, noOfMappings)
          .mapToObj(i -> TerminologyHelpers.mockCoding(toSystem, sourceCoding.getCode(), i))
          .map(coding -> Translation.of(ConceptMapEquivalence.EQUIVALENT, coding))
          .toArray(Translation[]::new);

      return withTranslations(sourceCoding, conceptMapUrl, false, translations);
    }

    public TranslateExpectations withTranslations(
        @Nonnull final Coding coding,
        @Nonnull final String conceptMapUrl,
        final boolean reverse, @Nonnull final Translation... translations) {

      return withTranslations(coding, conceptMapUrl, reverse, null, translations);
    }

    public TranslateExpectations withTranslations(
        @Nonnull final Coding coding,
        @Nonnull final String conceptMapUrl,
        final boolean reverse,
        @Nullable final String target,
        @Nonnull final Translation... translations) {

      when(
          mockService.translate(codingEq(coding), eq(conceptMapUrl), eq(reverse),
              eq(target))).thenReturn(List.of(translations));
      return this;
    }
  }

  public static class SubsumesExpectations {

    @Nonnull
    private final TerminologyService2 mockService;

    private static class DefaultAnswer implements Answer<ConceptSubsumptionOutcome> {

      @Override
      public ConceptSubsumptionOutcome answer(final InvocationOnMock invocationOnMock) {
        final Coding codingA = invocationOnMock.getArgument(0);
        final Coding codingB = invocationOnMock.getArgument(1);

        if (codingA != null && codingEquals(codingA, codingB)) {
          return EQUIVALENT;
        } else {
          return NOTSUBSUMED;
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
      when(mockService.subsumes(codingEq(codingA), codingEq(codingB))).thenReturn(SUBSUMES);
      when(mockService.subsumes(codingEq(codingB), codingEq(codingA))).thenReturn(
          SUBSUMEDBY);
      return this;
    }
  }


  public static class LookupExpectations {

    private final TerminologyService2 mockService;

    public LookupExpectations(final TerminologyService2 mockService) {
      this.mockService = mockService;
      clearInvocations(mockService);
      when(mockService.lookup(any(), any(), any())).thenReturn(Collections.emptyList());
    }

    @Nonnull
    public LookupExpectations withDisplay(@Nonnull final Coding coding,
        @Nonnull final String displayName) {
      when(mockService.lookup(codingEq(coding), eq("display"), any()))
          .thenReturn(List.of(
              Property.of("display", new StringType(displayName))));
      return this;
    }
    
    @Nonnull
    public LookupExpectations withDisplay(@Nonnull final Coding coding) {
      return withDisplay(coding, coding.getDisplay());
    }
  }

  @Nonnull
  public static ValidateExpectations setupValidate(@Nonnull final TerminologyService2 mockService) {
    return new ValidateExpectations(mockService);
  }


  @Nonnull
  public static TranslateExpectations setupTranslate(
      @Nonnull final TerminologyService2 mockService) {
    return new TranslateExpectations(mockService);
  }


  @Nonnull
  public static SubsumesExpectations setupSubsumes(
      @Nonnull final TerminologyService2 mockService) {
    return new SubsumesExpectations(mockService);
  }

  public static LookupExpectations setupLookup(@Nonnull final TerminologyService2 mockService) {
    return new LookupExpectations(mockService);
  }

}
