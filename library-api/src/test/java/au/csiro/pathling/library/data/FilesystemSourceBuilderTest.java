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

package au.csiro.pathling.library.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.TestHelpers;
import au.csiro.pathling.query.EnumerableDataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import com.google.common.collect.ImmutableSet;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.BiFunction;
import javax.annotation.Nonnull;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class FilesystemSourceBuilderTest {

  protected static final Path TEST_DATA_PATH = Path.of(
      "src/test/resources/test-data").toAbsolutePath().normalize();


  static final Path MIXED_TEST_DATA_PATH = TEST_DATA_PATH.resolve("mixed");

  static PathlingContext pathlingCtx;
  static SparkSession spark;
  static Dataset<Row> patientDf;
  static Dataset<Row> conditionDf;
  static Dataset<Row> observationDf;


  private final DataFrameReader mockDataFrameReader = mock(DataFrameReader.class);

  /**
   * Set up Spark.
   */
  @BeforeAll
  public static void setupContext() {
    spark = TestHelpers.spark();

    final TerminologyServiceFactory terminologyServiceFactory = mock(
        TerminologyServiceFactory.class, withSettings().serializable());

    pathlingCtx = PathlingContext.create(spark, FhirEncoders.forR4().getOrCreate(),
        terminologyServiceFactory);

    patientDf = spark.emptyDataset(
        pathlingCtx.getFhirEncoders().of(Patient.class)).toDF();
    conditionDf = spark.emptyDataset(
        pathlingCtx.getFhirEncoders().of(Condition.class)).toDF();
    observationDf = spark.emptyDataset(
        pathlingCtx.getFhirEncoders().of(Observation.class)).toDF();
  }

  /**
   * Tear down Spark.
   */
  @AfterAll
  public static void tearDownAll() {
    spark.stop();
  }

  static class ScalaSeqMatcher implements ArgumentMatcher<Seq<String>> {

    final Seq<String> expected;

    ScalaSeqMatcher(final Seq<String> expected) {
      this.expected = expected;
    }

    @Override
    public boolean matches(final Seq<String> actual) {
      return ((Object) expected).equals(actual);
    }
  }

  static Seq<String> seqEq(final String... expected) {
    final Seq<String> expectedSeq = JavaConverters.asScalaBuffer(
        Arrays.asList(expected)).toSeq();

    return Mockito.argThat(
        new ScalaSeqMatcher(expectedSeq)
    );
  }

  @Nonnull
  private static String toTestDataUri(String other) {
    return "file:" + MIXED_TEST_DATA_PATH.resolve(other);
  }


  @Test
  public void testBuildsEmptySourceWhenNoMatchingFiles() {
    final ReadableSource source = new FilesystemSourceBuilder(pathlingCtx)
        .withFilesGlob(MIXED_TEST_DATA_PATH.resolve("*.other").toString())
        .withReader(mockDataFrameReader)
        .build();
    assertEquals(Collections.emptySet(),
        ((EnumerableDataSource) source.getDataSource()).getDefinedResources());
    verifyNoInteractions(mockDataFrameReader);
  }


  @Test
  public void testOnlyLoadsMatchingFiles() {
    when(mockDataFrameReader.load(seqEq(toTestDataUri("Patient.ndjson")))).thenReturn(patientDf);
    when(mockDataFrameReader.load(seqEq(toTestDataUri("Observation.ndjson")))).thenReturn(
        observationDf);

    final ReadableSource source = new FilesystemSourceBuilder(pathlingCtx)
        .withFilesGlob(MIXED_TEST_DATA_PATH.resolve("*.ndjson").toString())
        .withReader(mockDataFrameReader)
        .build();

    assertEquals(ImmutableSet.of(ResourceType.PATIENT, ResourceType.OBSERVATION),
        ((EnumerableDataSource) source.getDataSource()).getDefinedResources());
    assertEquals(patientDf, source.getDataSource().read(ResourceType.PATIENT));
    assertEquals(observationDf, source.getDataSource().read(ResourceType.OBSERVATION));
  }


  @Test
  public void testCustomManyToManyFilenameMapper() {
    when(mockDataFrameReader.load(seqEq(
        toTestDataUri("Condition.xml"),
        toTestDataUri("Observation.ndjson"),
        toTestDataUri("Patient.ndjson"),
        toTestDataUri("Patient.xml")
    ))).thenReturn(patientDf);
    when(mockDataFrameReader.load(seqEq(
        toTestDataUri("Observation.ndjson"),
        toTestDataUri("Patient.ndjson")
    ))).thenReturn(conditionDf);
    when(mockDataFrameReader.load(seqEq(
        toTestDataUri("Condition.xml"),
        toTestDataUri("Patient.xml")
    ))).thenReturn(observationDf);

    final ReadableSource source = new FilesystemSourceBuilder(pathlingCtx)
        .withFilesGlob(MIXED_TEST_DATA_PATH.resolve("*.*").toString())
        .withReader(mockDataFrameReader)
        // This is purposely reverted, i.e. the actual file is Observation.ndjson, not Observation.xml.
        .withFilenameMapper(fn -> fn.endsWith("xml")
                                  ? Arrays.asList("Patient", "Observation")
                                  : Arrays.asList("Patient", "Condition"))
        .build();

    assertEquals(
        ImmutableSet.of(ResourceType.PATIENT, ResourceType.OBSERVATION, ResourceType.CONDITION),
        ((EnumerableDataSource) source.getDataSource()).getDefinedResources());
    assertEquals(patientDf, source.getDataSource().read(ResourceType.PATIENT));
    assertEquals(conditionDf, source.getDataSource().read(ResourceType.CONDITION));
    assertEquals(observationDf, source.getDataSource().read(ResourceType.OBSERVATION));
  }

  @Test
  public void testCustomTransformer() {

    // This is swapped on purpose. The transformer should correct that.
    when(mockDataFrameReader.load(seqEq(
        toTestDataUri("Condition.xml")
    ))).thenReturn(patientDf);
    when(mockDataFrameReader.load(seqEq(
        toTestDataUri("Patient.xml")
    ))).thenReturn(conditionDf);

    final BiFunction<Dataset<Row>, String, Dataset<Row>> mockTransformer = mock(BiFunction.class);

    when(mockTransformer.apply(eq(conditionDf), eq("Patient"))).thenReturn(patientDf);
    when(mockTransformer.apply(eq(patientDf), eq("Condition"))).thenReturn(conditionDf);

    final ReadableSource source = new FilesystemSourceBuilder(pathlingCtx)
        .withFilesGlob(MIXED_TEST_DATA_PATH.resolve("*.xml").toString())
        .withReader(mockDataFrameReader)
        .withDatasetTransformer(mockTransformer)
        .build();

    assertEquals(
        ImmutableSet.of(ResourceType.PATIENT, ResourceType.CONDITION),
        ((EnumerableDataSource) source.getDataSource()).getDefinedResources());
    assertEquals(patientDf, source.getDataSource().read(ResourceType.PATIENT));
    assertEquals(conditionDf, source.getDataSource().read(ResourceType.CONDITION));
  }
}
