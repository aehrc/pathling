/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.subsumes;

import static au.csiro.pathling.test.helpers.SparkHelpers.rowsFromSimpleCodings;
import static au.csiro.pathling.test.helpers.SparkHelpers.simpleCodingStructType;
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
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.fixtures.ConceptMapFixtures;
import ca.uhn.fhir.rest.param.UriParam;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.CodeSystem;
import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

@Tag("UnitTest")
public class SubsumptionMapperTest {

  private static final String SYSTEM1 = "uuid:system1";
  private static final String SYSTEM2 = "uuid:system2";
  private static final SimpleCoding CODING1_UNVERSIONED = new SimpleCoding(SYSTEM1, "code");
  private static final SimpleCoding CODING1_VERSION1 =
      new SimpleCoding(SYSTEM1, "code", "version1");
  private static final SimpleCoding CODING2_VERSION1 =
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
  @SuppressWarnings("ConstantConditions")
  public void testFiltersOutCodingsNotRecognizedByTerminologyServer() {

    // setup SYSTEM1 as known system
    when(terminologyClient.searchCodeSystems(refEq(new UriParam(SYSTEM1)), any()))
        .thenReturn(Collections.singletonList(new CodeSystem()));

    final SubsumptionMapper mapper = new SubsumptionMapper("foo", terminologyClientFactory, false);
    final List<Row> input = new DatasetBuilder()
        .withColumn("_abc123", DataTypes.StringType)
        .withColumn("id", DataTypes.StringType)
        .withColumn("inputCodings", DataTypes.createArrayType(simpleCodingStructType()))
        .withColumn("argCodings", DataTypes.createArrayType(simpleCodingStructType()))
        .withRow("female", "Patient/1", rowsFromSimpleCodings(CODING1_VERSION1),
            rowsFromSimpleCodings(CODING2_VERSION1))
        .withRow("male", "Patient/2", rowsFromSimpleCodings(CODING1_UNVERSIONED),
            rowsFromSimpleCodings(CODING1_VERSION1))
        .build()
        .collectAsList();

    final Iterator<Row> result = mapper.call(input.iterator());

    final List<Row> expected = new DatasetBuilder()
        .withColumn("_abc123", DataTypes.StringType)
        .withColumn("id", DataTypes.StringType)
        .withColumn("inputCodings", DataTypes.createArrayType(simpleCodingStructType()))
        .withColumn("argCodings", DataTypes.createArrayType(simpleCodingStructType()))
        .withColumn("value", DataTypes.BooleanType)
        .withRow("female", "Patient/1", rowsFromSimpleCodings(CODING1_VERSION1),
            rowsFromSimpleCodings(CODING2_VERSION1), false)
        .withRow("male", "Patient/2", rowsFromSimpleCodings(CODING1_UNVERSIONED),
            rowsFromSimpleCodings(CODING1_VERSION1), true)
        .build()
        .collectAsList();
    assertEquals(expected, IteratorUtils.toList(result));

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

    final SubsumptionMapper mapper = new SubsumptionMapper("foo", terminologyClientFactory, false);
    final List<Row> input = new DatasetBuilder()
        .withColumn("_abc123", DataTypes.StringType)
        .withColumn("id", DataTypes.StringType)
        .withColumn("inputCodings", DataTypes.createArrayType(simpleCodingStructType()))
        .withColumn("argCodings", DataTypes.createArrayType(simpleCodingStructType()))
        .withRow("female", "Patient/1", null, null)
        .withRow("male", "Patient/2", null, Collections.emptyList())
        .withRow("female", "Patient/3", Collections.emptyList(), null)
        .build()
        .collectAsList();
    final Iterator<Row> result = mapper.call(input.iterator());

    final List<Row> expected = new DatasetBuilder()
        .withColumn("_abc123", DataTypes.StringType)
        .withColumn("id", DataTypes.StringType)
        .withColumn("inputCodings", DataTypes.createArrayType(simpleCodingStructType()))
        .withColumn("argCodings", DataTypes.createArrayType(simpleCodingStructType()))
        .withColumn("value", DataTypes.BooleanType)
        .withRow("female", "Patient/1", null, null, null)
        .withRow("male", "Patient/2", null, Collections.emptyList(), null)
        .withRow("female", "Patient/3", Collections.emptyList(), null, false)
        .build()
        .collectAsList();
    assertEquals(expected, IteratorUtils.toList(result));

    // verify behaviour
    verify(terminologyClient).closure(any(), isNull());
    verify(terminologyClient)
        .closure(any(),
            argThat(new CodingSetMatcher(Collections.emptySet())));
    verifyNoMoreInteractions(terminologyClient);
  }

  @Test
  public void testFiltersOutUndefinedCodings() {

    final SubsumptionMapper mapper = new SubsumptionMapper("foo", terminologyClientFactory, false);
    final List<Row> input = new DatasetBuilder()
        .withColumn("_abc123", DataTypes.StringType)
        .withColumn("id", DataTypes.StringType)
        .withColumn("inputCodings", DataTypes.createArrayType(simpleCodingStructType()))
        .withColumn("argCodings", DataTypes.createArrayType(simpleCodingStructType()))
        .withRow("female", "Patient/1", rowsFromSimpleCodings(new SimpleCoding(SYSTEM1, null)),
            rowsFromSimpleCodings(new SimpleCoding(SYSTEM1, null)))
        .withRow("male", "Patient/2", rowsFromSimpleCodings(new SimpleCoding(null, "code1")),
            rowsFromSimpleCodings(new SimpleCoding(null, "code1")))
        .build()
        .collectAsList();
    final Iterator<Row> result = mapper.call(input.iterator());

    final List<Row> expected = new DatasetBuilder()
        .withColumn("_abc123", DataTypes.StringType)
        .withColumn("id", DataTypes.StringType)
        .withColumn("inputCodings", DataTypes.createArrayType(simpleCodingStructType()))
        .withColumn("argCodings", DataTypes.createArrayType(simpleCodingStructType()))
        .withColumn("value", DataTypes.BooleanType)
        .withRow("female", "Patient/1", rowsFromSimpleCodings(new SimpleCoding(SYSTEM1, null)),
            rowsFromSimpleCodings(new SimpleCoding(SYSTEM1, null)), false)
        .withRow("male", "Patient/2", rowsFromSimpleCodings(new SimpleCoding(null, "code1")),
            rowsFromSimpleCodings(new SimpleCoding(null, "code1")), false)
        .build()
        .collectAsList();
    assertEquals(expected, IteratorUtils.toList(result));

    // verify behaviour
    verify(terminologyClient).closure(any(), isNull());
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



