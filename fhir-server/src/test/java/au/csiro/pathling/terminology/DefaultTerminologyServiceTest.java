/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.terminology;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.test.fixtures.ConceptMapFixtures;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.param.UriParam;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.r4.model.CodeSystem;
import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@Tag("UnitTest")
@SpringBootTest
public class DefaultTerminologyServiceTest {

  private static final String SYSTEM1 = "uuid:system1";
  private static final String SYSTEM2 = "uuid:system2";
  private static final SimpleCoding CODING1_UNVERSIONED = new SimpleCoding(SYSTEM1, "code");
  private static final SimpleCoding CODING1_VERSION1 =
      new SimpleCoding(SYSTEM1, "code", "version1");
  private static final SimpleCoding CODING2_VERSION1 =
      new SimpleCoding(SYSTEM2, "code", "version1");


  @Autowired
  private FhirContext fhirContext;

  private TerminologyClient terminologyClient;
  private DefaultTerminologyService terminologyService;

  @BeforeEach
  public void setUp() {
    terminologyClient = mock(TerminologyClient.class);
    when(terminologyClient.closure(any(), any()))
        .thenReturn(ConceptMapFixtures.creatEmptyConceptMap());
    terminologyService = new DefaultTerminologyService(fhirContext, terminologyClient);
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testSubsumesFiltersOutCodingsNotRecognizedByTerminologyServer() {

    // setup SYSTEM1 as known system
    when(terminologyClient.searchCodeSystems(refEq(new UriParam(SYSTEM1)), any()))
        .thenReturn(Collections.singletonList(new CodeSystem()));

    terminologyService.getSubsumesRelation(
        Arrays.asList(CODING1_VERSION1, CODING2_VERSION1, CODING1_UNVERSIONED, CODING1_VERSION1));

    // verify behaviour
    verify(terminologyClient).searchCodeSystems(refEq(new UriParam(SYSTEM1)), any());
    verify(terminologyClient).searchCodeSystems(refEq(new UriParam(SYSTEM2)), any());
    verify(terminologyClient).initialiseClosure(any());
    verify(terminologyClient)
        .closure(any(),
            argThat(new CodingSetMatcher(Arrays.asList(CODING1_VERSION1, CODING1_UNVERSIONED))));
    verifyNoMoreInteractions(terminologyClient);
  }

  @Test
  public void testSubsumesFiltersOutUndefinedCodings() {
    terminologyService.getSubsumesRelation(
        Arrays.asList(new SimpleCoding(SYSTEM1, null), new SimpleCoding(SYSTEM1, null),
            new SimpleCoding(null, "code1"), new SimpleCoding(null, "code1")));

    // verify behaviour
    verify(terminologyClient).initialiseClosure(any());
    verify(terminologyClient)
        .closure(any(),
            argThat(new CodingSetMatcher(Collections.emptySet())));
    verifyNoMoreInteractions(terminologyClient);
  }

  private static class CodingSetMatcher implements ArgumentMatcher<List<Coding>> {

    @Nonnull
    private final Set<SimpleCoding> leftSet;

    // constructors
    private CodingSetMatcher(@Nonnull final Set<SimpleCoding> leftSet) {
      this.leftSet = leftSet;
    }

    private CodingSetMatcher(@Nonnull final List<SimpleCoding> left) {
      this(new HashSet<>(left));
    }

    @Override
    public boolean matches(@Nullable final List<Coding> right) {
      return right != null &&
          leftSet.size() == right.size() &&
          leftSet.equals(right.stream().map(SimpleCoding::new).collect(Collectors.toSet()));
    }
  }

}



