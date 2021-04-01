/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.terminology;

import static au.csiro.pathling.test.helpers.FhirHelpers.deepEq;
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
import au.csiro.pathling.test.fixtures.ConceptTranslatorBuilder;
import au.csiro.pathling.test.fixtures.RelationBuilder;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.param.UriParam;
import com.google.common.collect.ImmutableSet;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.Bundle.HTTPVerb;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.ValueSet.ConceptReferenceComponent;
import org.hl7.fhir.r4.model.ValueSet.ConceptSetComponent;
import org.hl7.fhir.r4.model.ValueSet.ValueSetExpansionContainsComponent;
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


  private ValueSetExpansionContainsComponent fromSimpleCoding(
      @Nonnull final SimpleCoding simpleCoding) {
    final ValueSetExpansionContainsComponent result = new ValueSetExpansionContainsComponent();
    result.setSystem(simpleCoding.getSystem());
    result.setCode(simpleCoding.getCode());
    result.setVersion(simpleCoding.getVersion());
    return result;
  }

  @Autowired
  private FhirContext fhirContext;

  private TerminologyClient terminologyClient;

  private TerminologyService terminologyService;

  @BeforeEach
  public void setUp() {
    terminologyClient = mock(TerminologyClient.class);
    final UUIDFactory uuidFactory = mock(UUIDFactory.class);
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
        .initialiseClosure(deepEq(new StringType(TEST_UUID_AS_STRING)));
    verify(terminologyClient)
        .closure(deepEq(new StringType(TEST_UUID_AS_STRING)),
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


  @SuppressWarnings("ConstantConditions")
  @Test
  public void testIntersectFiltersIllegalAndUnknownCodings() {

    final ValueSet responseExpansion = new ValueSet();
    responseExpansion.getExpansion().getContains().addAll(Arrays.asList(
        fromSimpleCoding(CODING1_VERSION1),
        fromSimpleCoding(CODING2_VERSION1)
    ));

    when(terminologyClient.expand(any(), any()))
        .thenReturn(responseExpansion);

    // setup SYSTEM1 as known system
    when(terminologyClient.searchCodeSystems(refEq(new UriParam(SYSTEM1)), any()))
        .thenReturn(Collections.singletonList(new CodeSystem()));

    final Set<SimpleCoding> actualExpansion = terminologyService.intersect("uuid:value-set",
        Arrays.asList(CODING1_VERSION1, CODING2_VERSION1, CODING3_VERSION1,
            new SimpleCoding(SYSTEM1, null), new SimpleCoding(null, "code1"),
            new SimpleCoding(null, null), null));

    final Set<SimpleCoding> expectedExpansion = ImmutableSet.of(CODING1_VERSION1, CODING2_VERSION1);
    assertEquals(expectedExpansion, actualExpansion);

    // verify behaviour
    verify(terminologyClient).searchCodeSystems(refEq(new UriParam(SYSTEM1)), any());
    verify(terminologyClient).searchCodeSystems(refEq(new UriParam(SYSTEM2)), any());

    final ValueSet requestValueSet = new ValueSet();
    final List<ConceptSetComponent> includes = requestValueSet.getCompose().getInclude();
    includes.add(new ConceptSetComponent().addValueSet("uuid:value-set").setSystem(SYSTEM1)
        .setVersion("version1").addConcept(new ConceptReferenceComponent().setCode("code1"))
        .addConcept(new ConceptReferenceComponent().setCode("code2")));

    verify(terminologyClient).expand(deepEq(requestValueSet), deepEq(new IntegerType(2)));
    verifyNoMoreInteractions(terminologyClient);
  }

  @Test
  public void testIntersectForEmptySet() {
    // Does NOT call the terminologyClient and returns equality relation
    final Set<SimpleCoding> actualExpansion = terminologyService
        .intersect("uuid:test", Collections.emptySet());
    assertEquals(Collections.emptySet(), actualExpansion);
    verifyNoMoreInteractions(terminologyClient);
  }

  @Test
  public void testTranslateForEmptyCodingSet() {
    // Does NOT call the terminologyClient and returns equality relation
    final ConceptTranslator actualTranslator = terminologyService
        .translate(Collections.emptySet(), "uuid:concept-map", false, Arrays
            .asList(ConceptMapEquivalence.values()));
    assertEquals(ConceptTranslatorBuilder.empty().build(), actualTranslator);
    verifyNoMoreInteractions(terminologyClient);
  }

  @Test
  public void testTranslateForEmptyEquivalences() {
    // Does NOT call the terminologyClient and returns equality relation
    final ConceptTranslator actualTranslator = terminologyService
        .translate(Arrays.asList(CODING1_VERSION1, CODING1_UNVERSIONED, CODING2_VERSION1),
            "uuid:concept-map", false, Collections.emptyList());
    assertEquals(ConceptTranslatorBuilder.empty().build(), actualTranslator);
    verifyNoMoreInteractions(terminologyClient);
  }

  @Test
  public void testTranslateForValidAndInvalidCodings() {

    // Response bundle:
    // [1]  CODING1_VERSION1 -> None
    // [2]  CODING2_VERSION1 -> { equivalent: CODING3_VERSION1, wider: CODING1_VERSION1}
    final Bundle responseBundle = new Bundle().setType(BundleType.BATCHRESPONSE);
    // entry with no mapping
    final Parameters noTranslation = new Parameters().addParameter("result", false);
    responseBundle.addEntry().setResource(noTranslation).getResponse().setStatus("200");

    // entry with two mappings
    final Parameters withTranslation = new Parameters().addParameter("result", true);
    final ParametersParameterComponent equivalentMatch = withTranslation.addParameter()
        .setName("match");
    equivalentMatch.addPart().setName("equivalence")
        .setValue(new CodeType("equivalent"));
    equivalentMatch.addPart().setName("concept")
        .setValue(CODING3_VERSION1.toCoding());

    final ParametersParameterComponent widerMatch = withTranslation.addParameter()
        .setName("match");
    widerMatch.addPart().setName("equivalence")
        .setValue(new CodeType("wider"));
    widerMatch.addPart().setName("concept")
        .setValue(CODING1_VERSION1.toCoding());

    responseBundle.addEntry().setResource(withTranslation).getResponse().setStatus("200");

    when(terminologyClient.batch(any())).thenReturn(responseBundle);
    final ConceptTranslator actualTranslator = terminologyService
        .translate(Arrays
                .asList(CODING1_VERSION1, CODING2_VERSION1, new SimpleCoding(SYSTEM1, null),
                    new SimpleCoding(null, "code1"),
                    new SimpleCoding(null, null), null),
            "uuid:concept-map", false,
            Collections.singletonList(ConceptMapEquivalence.EQUIVALENT));
    assertEquals(
        ConceptTranslatorBuilder.empty().put(CODING2_VERSION1, CODING3_VERSION1.toCoding()).build(),
        actualTranslator);

    // expected request bundle
    final Bundle requestBundle = new Bundle().setType(BundleType.BATCH);

    requestBundle.addEntry().setResource(
        new Parameters()
            .addParameter("url", new UriType("uuid:concept-map"))
            .addParameter("reverse", false)
            .addParameter("coding", CODING1_VERSION1.toCoding())
    ).getRequest().setMethod(HTTPVerb.POST)
        .setUrl("ConceptMap/$translate");

    requestBundle.addEntry().setResource(
        new Parameters()
            .addParameter("url", new UriType("uuid:concept-map"))
            .addParameter("reverse", false)
            .addParameter("coding", CODING2_VERSION1.toCoding())
    ).getRequest().setMethod(HTTPVerb.POST)
        .setUrl("ConceptMap/$translate");

    verify(terminologyClient).batch(deepEq(requestBundle));
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



