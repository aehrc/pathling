package au.csiro.pathling.test.helpers;

import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TranslateMapping.TranslationEntry;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.ValueSet;
import javax.annotation.Nonnull;

import java.util.stream.IntStream;

import static au.csiro.pathling.test.helpers.FhirMatchers.codingEq;
import static au.csiro.pathling.test.helpers.FhirMatchers.deepEq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class TerminologyServiceHelpers {

  public final static Parameters RESULT_TRUE = new Parameters().setParameter("result", true);
  public final static Parameters RESULT_FALSE = new Parameters().setParameter("result", false);


  public static class ValidateExpectations {

    private final @Nonnull
    TerminologyService mockService;

    ValidateExpectations(@Nonnull final TerminologyService mockService) {
      this.mockService = mockService;
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
    TerminologyService mockService;

    TranslateExpectations(@Nonnull final TerminologyService mockService) {
      this.mockService = mockService;
      when(mockService.translateCoding(any(), any(), anyBoolean())).thenReturn(RESULT_FALSE);
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
          mockService.translateCoding(codingEq(coding), eq(conceptMapUrl), eq(reverse))).thenReturn(
          translateResponse);
      return this;
    }
  }

  public static ValidateExpectations setupValidate(final @Nonnull TerminologyService mockService) {
    return new ValidateExpectations(mockService);
  }


  public static TranslateExpectations setupTranslate(
      final @Nonnull TerminologyService mockService) {
    return new TranslateExpectations(mockService);
  }

}
