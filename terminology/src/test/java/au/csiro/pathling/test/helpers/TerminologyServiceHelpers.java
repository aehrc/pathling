/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.test.helpers;

import static au.csiro.pathling.fhirpath.CodingHelpers.codingEquals;
import static au.csiro.pathling.test.helpers.FhirMatchers.codingEq;
import static au.csiro.pathling.test.helpers.FhirMatchers.deepEq;
import static org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome.EQUIVALENT;
import static org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome.NOTSUBSUMED;
import static org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome.SUBSUMEDBY;
import static org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome.SUBSUMES;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhirpath.encoding.ImmutableCoding;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyService.Designation;
import au.csiro.pathling.terminology.TerminologyService.Property;
import au.csiro.pathling.terminology.TerminologyService.PropertyOrDesignation;
import au.csiro.pathling.terminology.TerminologyService.Translation;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.Type;
import org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TerminologyServiceHelpers {

  public static final Parameters RESULT_TRUE = new Parameters().setParameter("result", true);
  public static final Parameters RESULT_FALSE = new Parameters().setParameter("result", false);

  public static final Parameters OUTCOME_EQUIVALENT = new Parameters().setParameter("outcome",
      EQUIVALENT.toCode());
  public static final Parameters OUTCOME_SUBSUMES = new Parameters().setParameter("outcome",
      SUBSUMES.toCode());
  public static final Parameters OUTCOME_SUBSUMED_BY = new Parameters().setParameter("outcome",
      SUBSUMEDBY.toCode());

  public static class ValidateExpectations {

    @Nonnull
    private final TerminologyService mockService;

    ValidateExpectations(@Nonnull final TerminologyService mockService) {
      this.mockService = mockService;
      clearInvocations(mockService);
      when(mockService.validateCode(any(), any())).thenReturn(false);
    }

    @Nonnull
    public ValidateExpectations withValueSet(@Nonnull final String valueSetUrl,
        @Nonnull final Coding... codings) {
      for (final Coding coding : codings) {
        when(mockService.validateCode(eq(valueSetUrl), codingEq(coding))).thenReturn(true);
      }
      return this;
    }
  }

  public static class TranslateExpectations {

    @Nonnull
    private final TerminologyService mockService;

    TranslateExpectations(@Nonnull final TerminologyService mockService) {
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
    private final TerminologyService mockService;

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

    public SubsumesExpectations(@Nonnull final TerminologyService mockService) {
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

    private final Map<ImmutableCoding, List<PropertyOrDesignation>> designationsOfCoding = new HashMap<>();

    private final TerminologyService mockService;

    public LookupExpectations(final TerminologyService mockService) {
      this.mockService = mockService;
      clearInvocations(mockService);
      when(mockService.lookup(any(), any())).thenReturn(Collections.emptyList());
    }

    @Nonnull
    public LookupExpectations withDisplay(@Nonnull final Coding coding,
        @Nonnull final String displayName) {
      when(mockService.lookup(codingEq(coding), eq("display"), eq(null)))
          .thenReturn(List.of(
              Property.of("display", new StringType(displayName))));
      return this;
    }

    @Nonnull
    public LookupExpectations withDisplay(@Nonnull final Coding coding) {
      return withDisplay(coding, coding.getDisplay());
    }

    @Nonnull
    public LookupExpectations withDisplay(@Nonnull final Coding coding,
        @Nonnull final String displayName, @Nullable final String acceptLanguage) {
      when(mockService.lookup(codingEq(coding), eq("display"), eq(acceptLanguage)))
          .thenReturn(List.of(
              Property.of("display", new StringType(displayName))));
      return this;
    }


    @Nonnull
    public final <T extends Type> LookupExpectations withProperty(@Nonnull final Coding coding,
        @Nonnull final String propertyCode, @Nullable final String displayLanguage,
        final List<T> values) {
      when(mockService.lookup(deepEq(coding), eq(propertyCode), eq(displayLanguage))).thenReturn(
          values.stream()
              .map(v -> (PropertyOrDesignation) Property.of(propertyCode, v))
              .toList()
      );
      return this;
    }

    @SafeVarargs
    @Nonnull
    public final <T extends Type> LookupExpectations withProperty(@Nonnull final Coding coding,
        @Nonnull final String propertyCode, @Nullable final String displayLanguage,
        final T... value) {
      return withProperty(coding, propertyCode, displayLanguage, List.of(value));
    }

    public LookupExpectations withDesignation(@Nonnull final Coding coding,
        @Nullable final Coding use,
        @Nullable final String language, @Nonnull final String... designations) {

      final List<PropertyOrDesignation> currentDesignations = designationsOfCoding.computeIfAbsent(
          ImmutableCoding.of(coding),
          c -> new ArrayList<>());
      Stream.of(designations).forEach(
          designation -> currentDesignations.add(Designation.of(use, language, designation)));
      return this;
    }

    public void done() {
      designationsOfCoding.forEach((coding, designations) -> when(
          mockService.lookup(deepEq(coding.toCoding()), eq("designation")))
          .thenReturn(designations));
    }
  }

  @Nonnull
  public static ValidateExpectations setupValidate(@Nonnull final TerminologyService mockService) {
    return new ValidateExpectations(mockService);
  }


  @Nonnull
  public static TranslateExpectations setupTranslate(
      @Nonnull final TerminologyService mockService) {
    return new TranslateExpectations(mockService);
  }


  @Nonnull
  public static SubsumesExpectations setupSubsumes(
      @Nonnull final TerminologyService mockService) {
    return new SubsumesExpectations(mockService);
  }

  public static LookupExpectations setupLookup(@Nonnull final TerminologyService mockService) {
    return new LookupExpectations(mockService);
  }

}
