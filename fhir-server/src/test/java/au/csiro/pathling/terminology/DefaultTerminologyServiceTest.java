/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.terminology;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.test.fixtures.ConceptMapBuilder;
import au.csiro.pathling.test.fixtures.RelationBuilder;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.param.UriParam;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.r4.model.CodeSystem;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.StringType;
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
  private static final SimpleCoding CODING1_UNVERSIONED = new SimpleCoding(SYSTEM1, "code1");
  private static final SimpleCoding CODING1_VERSION1 =
      new SimpleCoding(SYSTEM1, "code1", "version1");
  private static final SimpleCoding CODING2_VERSION1 =
      new SimpleCoding(SYSTEM1, "code2", "version1");
  private static final SimpleCoding CODING3_VERSION1 =
      new SimpleCoding(SYSTEM2, "code", "version1");

  private static final String TEST_UUID_AS_STRING = "5d1b976d-c50c-445a-8030-64074b83f355";
  private static final UUID TEST_UUID = UUID.fromString(TEST_UUID_AS_STRING);

  @Autowired
  private FhirContext fhirContext;

  private TerminologyClient terminologyClient;
  private UUIDFactory uuidFactory;

  private DefaultTerminologyService terminologyService;

  @BeforeEach
  public void setUp() {
    terminologyClient = mock(TerminologyClient.class);
    uuidFactory = mock(UUIDFactory.class);
    when(uuidFactory.nextUUID()).thenReturn(TEST_UUID);
    terminologyService = new DefaultTerminologyService(fhirContext, terminologyClient, uuidFactory);
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testSubsumesFiltersIllegalAndUnknownCodings() {

    // CODING1 subsumes CODING2
    final ConceptMap responseMap = ConceptMapBuilder.empty()
        .withSubsumes(CODING2_VERSION1.toCoding(),
            CODING1_VERSION1.toCoding()).build();

    when(terminologyClient.closure(any(), any()))
        .thenReturn(responseMap);

    // setup SYSTEM1 as known system
    when(terminologyClient.searchCodeSystems(refEq(new UriParam(SYSTEM1)), any()))
        .thenReturn(Collections.singletonList(new CodeSystem()));

    final Relation actualRelation = terminologyService.getSubsumesRelation(
        Arrays.asList(CODING1_VERSION1, CODING1_UNVERSIONED, CODING2_VERSION1, CODING3_VERSION1,
            new SimpleCoding(SYSTEM1, null), new SimpleCoding(null, "code1"),
            new SimpleCoding(null, null), null));

    final Relation expectedRelation = RelationBuilder.empty()
        .add(CODING1_VERSION1.toCoding(), CODING2_VERSION1.toCoding()).build();

    assertEquals(expectedRelation, actualRelation);

    // verify behaviour
    verify(terminologyClient).searchCodeSystems(refEq(new UriParam(SYSTEM1)), any());
    verify(terminologyClient).searchCodeSystems(refEq(new UriParam(SYSTEM2)), any());
    verify(terminologyClient)
        .initialiseClosure(argThat(new StringTypeMatcher(TEST_UUID_AS_STRING)));
    verify(terminologyClient)
        .closure(argThat(new StringTypeMatcher(TEST_UUID_AS_STRING)),
            argThat(new CodingSetMatcher(
                Arrays.asList(CODING1_VERSION1, CODING1_UNVERSIONED, CODING2_VERSION1))));
    verifyNoMoreInteractions(terminologyClient);
  }

  @Test
  public void testSubsumeForEmptySet() {
    // Does NOT call the terminologyClient and returns equality relation
    final Relation actualRelation = terminologyService.getSubsumesRelation(Collections.emptySet());
    assertEquals(Relation.equality(), actualRelation);
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


  private static class StringTypeMatcher implements ArgumentMatcher<StringType> {

    @Nonnull
    private final String expected;

    public StringTypeMatcher(@Nonnull String expected) {
      this.expected = expected;
    }

    @Override
    public boolean matches(@Nullable final StringType actual) {
      return actual != null && expected.equals(actual.getValue());
    }
  }


}



