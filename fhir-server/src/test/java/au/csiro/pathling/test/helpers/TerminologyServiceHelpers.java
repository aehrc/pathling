package au.csiro.pathling.test.helpers;

import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TranslateMapping.TranslationEntry;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import javax.annotation.Nonnull;

import static au.csiro.pathling.test.helpers.FhirDeepMatcher.deepEq;
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

    public ValidateExpectations withValueSet(@Nonnull final String valueSetUrl,
        @Nonnull final Coding... codings) {
      for (Coding coding : codings) {
        when(mockService.validate(eq(valueSetUrl), deepEq(coding))).thenReturn(RESULT_TRUE);
      }
      return this;
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
      when(mockService.translateCoding(deepEq(coding), eq(conceptMapUrl), anyBoolean())).thenReturn(
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
