/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.subsumes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.fhirpath.encoding.BooleanResult;
import au.csiro.pathling.fhirpath.encoding.IdAndCodingSets;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.test.fixtures.ConceptMapFixtures;
import ca.uhn.fhir.rest.param.UriParam;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.collections4.IteratorUtils;
import org.hl7.fhir.r4.model.CodeSystem;
import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

@Tag("UnitTest")
public class SubsumptionMapperTest {

  static class CodingSetMatcher implements ArgumentMatcher<List<Coding>> {

    @Nonnull
    private final Set<SimpleCoding> leftSet;

    // constructors
    CodingSetMatcher(@Nonnull Set<SimpleCoding> leftSet) {
      this.leftSet = leftSet;
    }

    CodingSetMatcher(@Nonnull List<SimpleCoding> left) {
      this(new HashSet<>(left));
    }

    @Override
    public boolean matches(@Nullable List<Coding> right) {
      return right != null &&
          leftSet.size() == right.size() &&
          leftSet.equals(right.stream().map(SimpleCoding::new).collect(Collectors.toSet()));
    }
  }

  public static final String SYSTEM1 = "uuid:system1";
  public static final String SYSTEM2 = "uuid:system2";

  public static final SimpleCoding CODING1_UNVERSIONED = new SimpleCoding(SYSTEM1, "code");
  public static final SimpleCoding CODING1_VERSION1 =
      new SimpleCoding(SYSTEM1, "code", "version1");

  public static final SimpleCoding CODING2_VERSION1 =
      new SimpleCoding(SYSTEM2, "code", "version1");


  private TerminologyClient terminologyClient;
  private TerminologyClientFactory terminologyClientFactory;

  @BeforeEach
  public void setUp() {
    // NOTE: We need to make TerminologyClient mock serializable so that the TerminologyClientFactory
    // mock is serializable too.
    terminologyClient = mock(TerminologyClient.class, Mockito.withSettings().serializable());
    terminologyClientFactory = mock(TerminologyClientFactory.class,
        Mockito.withSettings().serializable());
    when(terminologyClientFactory.build(any())).thenReturn(terminologyClient);
    when(terminologyClient.closure(any(), any()))
        .thenReturn(ConceptMapFixtures.creatEmptyConceptMap());

  }

  @Test
  public void testFiltersOutCodingsNotReognizedByTerminlogyServer() {

    // setup SYSTEM1 as known system
    when(terminologyClient.searchCodeSystems(refEq(new UriParam(SYSTEM1)), any()))
        .thenReturn(Collections.singletonList(new CodeSystem()));

    SubsumptionMapper mapper = new SubsumptionMapper(terminologyClientFactory, false);
    List<IdAndCodingSets> input = Arrays.asList(
        new IdAndCodingSets("id1",
            Arrays.asList(CODING1_VERSION1), Arrays.asList(CODING2_VERSION1)),
        new IdAndCodingSets("id2",
            Arrays.asList(CODING1_UNVERSIONED), Arrays.asList(CODING1_VERSION1))
    );
    Iterator<BooleanResult> result = mapper
        .call(input.iterator());

    assertEquals(Arrays.asList(BooleanResult.of("id1", false), BooleanResult.of("id2", true)),
        IteratorUtils.toList(result));

    // verify behaviour
    verify(terminologyClient).searchCodeSystems(refEq(new UriParam(SYSTEM1)), any());
    verify(terminologyClient).searchCodeSystems(refEq(new UriParam(SYSTEM2)), any());
    verify(terminologyClient).closure(any(), isNull());
    verify(terminologyClient)
        .closure(any(),
            argThat(new CodingSetMatcher(Arrays.asList(CODING1_VERSION1, CODING1_UNVERSIONED))));
    verifyNoMoreInteractions(terminologyClient);
  }

  @Test
  public void testFiltersUndefineCodingSets() {

    SubsumptionMapper mapper = new SubsumptionMapper(terminologyClientFactory, false);
    List<IdAndCodingSets> input = Arrays.asList(
        new IdAndCodingSets("id-null-null",
            null, null),
        new IdAndCodingSets("id-null-empty",
            null, Collections.emptyList()),
        new IdAndCodingSets("id-empty-null", Collections.emptyList(), null)
    );
    Iterator<BooleanResult> result = mapper
        .call(input.iterator());

    assertEquals(Arrays.asList(BooleanResult.nullOf("id-null-null"),
        BooleanResult.nullOf("id-null-empty"),
        BooleanResult.of("id-empty-null", false)
        ),
        IteratorUtils.toList(result));

    // verify behaviour
    verify(terminologyClient).closure(any(), isNull());
    verify(terminologyClient)
        .closure(any(),
            argThat(new CodingSetMatcher(Collections.emptySet())));
    verifyNoMoreInteractions(terminologyClient);
  }

  @Test
  public void testFiltersOutUndefinedCodings() {

    SubsumptionMapper mapper = new SubsumptionMapper(terminologyClientFactory, false);
    List<IdAndCodingSets> input = Arrays.asList(
        new IdAndCodingSets("id-null-code",
            Arrays.asList(new SimpleCoding(SYSTEM1, null)),
            Arrays.asList(new SimpleCoding(SYSTEM1, null))),
        new IdAndCodingSets("id-null-system",
            Arrays.asList(new SimpleCoding(null, "code1")),
            Arrays.asList(new SimpleCoding(null, "code1")))
    );
    Iterator<BooleanResult> result = mapper
        .call(input.iterator());

    assertEquals(Arrays.asList(
        BooleanResult.of("id-null-code", false),
        BooleanResult.of("id-null-system", false)
        ),
        IteratorUtils.toList(result));

    // verify behaviour
    verify(terminologyClient).closure(any(), isNull());
    verify(terminologyClient)
        .closure(any(),
            argThat(new CodingSetMatcher(Collections.emptySet())));
    verifyNoMoreInteractions(terminologyClient);
  }
}



