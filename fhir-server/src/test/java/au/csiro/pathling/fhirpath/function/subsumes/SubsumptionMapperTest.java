/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.subsumes;

import org.junit.jupiter.api.Tag;

@Tag("UnitTest")
public class SubsumptionMapperTest {

  // private static final String SYSTEM1 = "uuid:system1";
  // private static final String SYSTEM2 = "uuid:system2";
  // private static final SimpleCoding CODING1_UNVERSIONED = new SimpleCoding(SYSTEM1, "code");
  // private static final SimpleCoding CODING1_VERSION1 =
  //     new SimpleCoding(SYSTEM1, "code", "version1");
  // private static final SimpleCoding CODING2_VERSION1 =
  //     new SimpleCoding(SYSTEM2, "code", "version1");
  // private TerminologyClient terminologyClient;
  // private TerminologyClientFactory terminologyClientFactory;
  //
  // @Test
  // @SuppressWarnings("ConstantConditions")
  // public void testFiltersOutCodingsNotRecognizedByTerminologyServer() {
  //
  //   // setup SYSTEM1 as known system
  //   when(terminologyClient.searchCodeSystems(refEq(new UriParam(SYSTEM1)), any()))
  //       .thenReturn(Collections.singletonList(new CodeSystem()));
  //
  //   final SubsumptionMapper mapper = new SubsumptionMapper("foo", terminologyClientFactory, false);
  //   final List<IdAndCodingSets> input = Arrays.asList(
  //       new IdAndCodingSets("id1", Collections.singletonList(CODING1_VERSION1),
  //           Collections.singletonList(CODING2_VERSION1)),
  //       new IdAndCodingSets("id2", Collections.singletonList(CODING1_UNVERSIONED),
  //           Collections.singletonList(CODING1_VERSION1))
  //   );
  //   final Iterator<BooleanResult> result = mapper
  //       .call(input.iterator());
  //
  //   assertEquals(Arrays.asList(BooleanResult.of("id1", false), BooleanResult.of("id2", true)),
  //       IteratorUtils.toList(result));
  //
  //   // verify behaviour
  //   verify(terminologyClient).searchCodeSystems(refEq(new UriParam(SYSTEM1)), any());
  //   verify(terminologyClient).searchCodeSystems(refEq(new UriParam(SYSTEM2)), any());
  //   verify(terminologyClient).closure(any(), isNull());
  //   verify(terminologyClient)
  //       .closure(any(),
  //           argThat(new CodingSetMatcher(Arrays.asList(CODING1_VERSION1, CODING1_UNVERSIONED))));
  //   verifyNoMoreInteractions(terminologyClient);
  // }
  //
  // @BeforeEach
  // public void setUp() {
  //   // NOTE: We need to make TerminologyClient mock serializable so that the TerminologyClientFactory
  //   // mock is serializable too.
  //   terminologyClient = mock(TerminologyClient.class, Mockito.withSettings().serializable());
  //   terminologyClientFactory = mock(TerminologyClientFactory.class,
  //       Mockito.withSettings().serializable());
  //   when(terminologyClientFactory.build(any())).thenReturn(terminologyClient);
  //   when(terminologyClient.closure(any(), any()))
  //       .thenReturn(ConceptMapFixtures.creatEmptyConceptMap());
  //
  // }
  //
  // @Test
  // public void testFiltersUndefineCodingSets() {
  //
  //   final SubsumptionMapper mapper = new SubsumptionMapper("foo", terminologyClientFactory, false);
  //   final List<IdAndCodingSets> input = Arrays.asList(
  //       new IdAndCodingSets("id-null-null",
  //           null, null),
  //       new IdAndCodingSets("id-null-empty",
  //           null, Collections.emptyList()),
  //       new IdAndCodingSets("id-empty-null", Collections.emptyList(), null)
  //   );
  //   final Iterator<BooleanResult> result = mapper
  //       .call(input.iterator());
  //
  //   assertEquals(Arrays.asList(BooleanResult.nullOf("id-null-null"),
  //       BooleanResult.nullOf("id-null-empty"),
  //       BooleanResult.of("id-empty-null", false)
  //       ),
  //       IteratorUtils.toList(result));
  //
  //   // verify behaviour
  //   verify(terminologyClient).closure(any(), isNull());
  //   verify(terminologyClient)
  //       .closure(any(),
  //           argThat(new CodingSetMatcher(Collections.emptySet())));
  //   verifyNoMoreInteractions(terminologyClient);
  // }
  //
  // @Test
  // public void testFiltersOutUndefinedCodings() {
  //
  //   final SubsumptionMapper mapper = new SubsumptionMapper("foo", terminologyClientFactory, false);
  //   final List<IdAndCodingSets> input = Arrays.asList(
  //       new IdAndCodingSets("id-null-code",
  //           Collections.singletonList(new SimpleCoding(SYSTEM1, null)),
  //           Collections.singletonList(new SimpleCoding(SYSTEM1, null))),
  //       new IdAndCodingSets("id-null-system",
  //           Collections.singletonList(new SimpleCoding(null, "code1")),
  //           Collections.singletonList(new SimpleCoding(null, "code1")))
  //   );
  //   final Iterator<BooleanResult> result = mapper.call(input.iterator());
  //
  //   assertEquals(Arrays.asList(
  //       BooleanResult.of("id-null-code", false),
  //       BooleanResult.of("id-null-system", false)
  //       ),
  //       IteratorUtils.toList(result));
  //
  //   // verify behaviour
  //   verify(terminologyClient).closure(any(), isNull());
  //   verify(terminologyClient)
  //       .closure(any(),
  //           argThat(new CodingSetMatcher(Collections.emptySet())));
  //   verifyNoMoreInteractions(terminologyClient);
  // }
  //
  // private static class CodingSetMatcher implements ArgumentMatcher<List<Coding>> {
  //
  //   @Nonnull
  //   private final Set<SimpleCoding> leftSet;
  //
  //   // constructors
  //   private CodingSetMatcher(@Nonnull final Set<SimpleCoding> leftSet) {
  //     this.leftSet = leftSet;
  //   }
  //
  //   private CodingSetMatcher(@Nonnull final List<SimpleCoding> left) {
  //     this(new HashSet<>(left));
  //   }
  //
  //   @Override
  //   public boolean matches(@Nullable final List<Coding> right) {
  //     return right != null &&
  //         leftSet.size() == right.size() &&
  //         leftSet.equals(right.stream().map(SimpleCoding::new).collect(Collectors.toSet()));
  //   }
  // }
}



