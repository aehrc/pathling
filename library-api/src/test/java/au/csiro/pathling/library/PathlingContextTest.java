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

package au.csiro.pathling.library;

import static au.csiro.pathling.test.SchemaAsserts.assertFieldNotPresent;
import static au.csiro.pathling.test.SchemaAsserts.assertFieldPresent;
import static java.util.function.Predicate.not;
import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import au.csiro.pathling.config.EncodingConfiguration;
import au.csiro.pathling.config.HttpClientCachingConfiguration;
import au.csiro.pathling.config.HttpClientCachingStorageType;
import au.csiro.pathling.config.HttpClientConfiguration;
import au.csiro.pathling.config.TerminologyAuthConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.terminology.DefaultTerminologyServiceFactory;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirVersionEnum;
import jakarta.annotation.Nonnull;
import jakarta.validation.ConstraintViolationException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.hl7.fhir.r4.model.Condition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.mutable.WrappedArray;

@Slf4j
public class PathlingContextTest {

  private static SparkSession spark;
  private static final String TEST_DATA_URL = "target/encoders-tests/data";

  /**
   * Set up Spark.
   */
  @BeforeAll
  static void setUpAll() {
    spark = TestHelpers.spark();
  }

  /**
   * Tear down Spark.
   */
  @AfterAll
  static void tearDownAll() {
    spark.stop();
  }

  private TerminologyServiceFactory terminologyServiceFactory;
  private TerminologyService terminologyService;


  private static final String GUID_REG_SUBEXPRESSION = "[0-9a-fA-F]{8}"
      + "-([0-9a-fA-F]{4}-)"
      + "{3}[0-9a-fA-F]{12}";


  public static final Pattern GUID_REGEX = Pattern.compile(
      "^" + GUID_REG_SUBEXPRESSION + "$");

  private static final Pattern RELATIVE_REF_REGEX = Pattern.compile(
      "^[A-Z][A-Za-z]+/" + GUID_REG_SUBEXPRESSION + "$");


  public static boolean isValidGUID(@Nonnull final String maybeGUID) {
    final Matcher m = GUID_REGEX.matcher(maybeGUID);
    return m.matches();
  }

  public static boolean isValidRelativeReference(@Nonnull final String maybeRelativeRef) {
    final Matcher m = RELATIVE_REF_REGEX.matcher(maybeRelativeRef);
    return m.matches();
  }

  public void assertAllGUIDs(@Nonnull final Collection<String> maybeGuids) {
    final List<String> invalidValues = maybeGuids.stream()
        .filter(not(PathlingContextTest::isValidGUID))
        .limit(7)
        .toList();
    assertTrue(invalidValues.isEmpty(),
        "All values should be GUIDs, but some are not: " + invalidValues);
  }

  public void assertAllRelativeReferences(@Nonnull final Collection<String> maybeRelativeRefs) {
    final List<String> invalidValues = maybeRelativeRefs.stream()
        .filter(not(PathlingContextTest::isValidRelativeReference))
        .limit(7)
        .toList();
    assertTrue(invalidValues.isEmpty(),
        "All values should be relative references, but some are not: " + invalidValues);
  }

  public <T> void assertValidIdColumns(@Nonnull final Dataset<T> df) {
    assertAllGUIDs(df.select("id").as(Encoders.STRING()).collectAsList());
    assertAllRelativeReferences(df.select("id_versioned").as(Encoders.STRING()).collectAsList());
  }

  public <T> void assertValidRelativeRefColumns(@Nonnull final Dataset<T> df,
      final Column... refColumns) {
    Stream.of(refColumns).forEach(c -> assertAllRelativeReferences(
        df.select(c.getField("reference")).as(Encoders.STRING()).collectAsList()));
  }

  @BeforeEach
  void setUp() {
    // setup terminology mocks
    terminologyServiceFactory = mock(
        TerminologyServiceFactory.class, withSettings().serializable());
    terminologyService = mock(TerminologyService.class,
        withSettings().serializable());
    when(terminologyServiceFactory.build()).thenReturn(terminologyService);

    DefaultTerminologyServiceFactory.reset();
  }


  @Test
  void testEncodeResourcesFromJsonBundle() {

    final Dataset<String> bundlesDF = spark.read().option("wholetext", true)
        .textFile(TEST_DATA_URL + "/bundles/R4/json");

    final PathlingContext pathling = PathlingContext.create(spark);

    final Dataset<Row> patientsDataframe = pathling.encodeBundle(bundlesDF.toDF(),
        "Patient", PathlingContext.FHIR_JSON);
    assertEquals(5, patientsDataframe.count());

    assertValidIdColumns(patientsDataframe);

    // Test omission of MIME type.
    final Dataset<Row> patientsDataframe2 = pathling.encodeBundle(bundlesDF.toDF(),
        "Patient");
    assertEquals(5, patientsDataframe2.count());
    assertValidIdColumns(patientsDataframe2);

    final Dataset<Condition> conditionsDataframe = pathling.encodeBundle(bundlesDF, Condition.class,
        PathlingContext.FHIR_JSON);
    assertEquals(107, conditionsDataframe.count());

    assertValidIdColumns(conditionsDataframe);
    assertValidRelativeRefColumns(conditionsDataframe, col("subject"));
    assertValidRelativeRefColumns(conditionsDataframe, col("encounter"));
  }


  @Test
  void testEncodeResourcesFromXmlBundle() {
    final Dataset<String> bundlesDF = spark.read().option("wholetext", true)
        .textFile(TEST_DATA_URL + "/bundles/R4/xml");

    final PathlingContext pathling = PathlingContext.create(spark);
    final Dataset<Condition> conditionsDataframe = pathling.encodeBundle(bundlesDF, Condition.class,
        PathlingContext.FHIR_XML);
    assertEquals(107, conditionsDataframe.count());

    assertValidIdColumns(conditionsDataframe);
    assertValidRelativeRefColumns(conditionsDataframe, col("subject"));
    assertValidRelativeRefColumns(conditionsDataframe, col("encounter"));
  }


  @Test
  void testEncodeResourcesFromJson() {
    final Dataset<String> jsonResources = spark.read()
        .textFile(TEST_DATA_URL + "/resources/R4/json");

    final PathlingContext pathling = PathlingContext.create(spark);

    final Dataset<Row> patientsDataframe = pathling.encode(jsonResources.toDF(), "Patient",
        PathlingContext.FHIR_JSON);
    assertEquals(9, patientsDataframe.count());
    assertValidIdColumns(patientsDataframe);

    final Dataset<Condition> conditionsDataframe = pathling.encode(jsonResources, Condition.class,
        PathlingContext.FHIR_JSON);
    assertEquals(71, conditionsDataframe.count());

    assertValidIdColumns(conditionsDataframe);
    assertValidRelativeRefColumns(conditionsDataframe, col("subject"));
    assertValidRelativeRefColumns(conditionsDataframe, col("encounter"));
  }

  @Test
  void testEncoderOptions() {
    final Dataset<Row> jsonResourcesDF = spark.read()
        .text(TEST_DATA_URL + "/resources/R4/json");

    // Test the defaults
    final Row defaultRow = PathlingContext.create(spark)
        .encode(jsonResourcesDF, "Questionnaire")
        .head();
    assertFieldPresent("_extension", defaultRow.schema());
    final Row defaultItem = (Row) defaultRow.getList(defaultRow.fieldIndex("item")).get(0);
    assertFieldPresent("item", defaultItem.schema());

    // Test explicit options
    // Nested items
    final EncodingConfiguration encodingConfig1 = EncodingConfiguration.builder()
        .enableExtensions(false)
        .maxNestingLevel(1)
        .build();
    final Row rowWithNesting = PathlingContext.create(spark, encodingConfig1)
        .encode(jsonResourcesDF, "Questionnaire").head();
    assertFieldNotPresent("_extension", rowWithNesting.schema());
    // Test item nesting
    final Row itemWithNesting = (Row) rowWithNesting
        .getList(rowWithNesting.fieldIndex("item")).get(0);
    assertFieldPresent("item", itemWithNesting.schema());
    final Row nestedItem = (Row) itemWithNesting
        .getList(itemWithNesting.fieldIndex("item")).get(0);
    assertFieldNotPresent("item", nestedItem.schema());

    // Test explicit options
    // Extensions and open types
    final EncodingConfiguration encodingConfig2 = EncodingConfiguration.builder()
        .enableExtensions(true)
        .openTypes(Set.of("boolean", "string", "Address"))
        .build();
    final Row rowWithExtensions = PathlingContext.create(spark, encodingConfig2)
        .encode(jsonResourcesDF, "Patient").head();
    assertFieldPresent("_extension", rowWithExtensions.schema());

    final Map<Integer, WrappedArray<Row>> extensions = rowWithExtensions
        .getJavaMap(rowWithExtensions.fieldIndex("_extension"));

    // get the first extension of some extension set
    final Row extension = (Row) extensions.values().toArray(WrappedArray[]::new)[0].apply(0);
    assertFieldPresent("valueString", extension.schema());
    assertFieldPresent("valueAddress", extension.schema());
    assertFieldPresent("valueBoolean", extension.schema());
    assertFieldNotPresent("valueInteger", extension.schema());
  }

  @Test
  void testEncodeResourceStream() throws Exception {
    final EncodingConfiguration encodingConfig = EncodingConfiguration.builder()
        .enableExtensions(true)
        .build();
    final PathlingContext pathling = PathlingContext.create(spark, encodingConfig);

    final Dataset<Row> jsonResources = spark.readStream()
        .text(TEST_DATA_URL + "/resources/R4/json");

    assertTrue(jsonResources.isStreaming());

    final Dataset<Row> patientsStream = pathling.encode(jsonResources, "Patient",
        PathlingContext.FHIR_JSON);

    assertTrue(patientsStream.isStreaming());

    final StreamingQuery patientsQuery = patientsStream
        .writeStream()
        .queryName("patients")
        .format("memory")
        .start();

    patientsQuery.processAllAvailable();
    final long patientsCount = spark.sql("select count(*) from patients").head().getLong(0);
    assertEquals(9, patientsCount);

    final StreamingQuery conditionQuery = pathling.encode(jsonResources, "Condition",
            PathlingContext.FHIR_JSON)
        .groupBy()
        .count()
        .writeStream()
        .outputMode(OutputMode.Complete())
        .queryName("countCondition")
        .format("memory")
        .start();

    conditionQuery.processAllAvailable();
    final long conditionsCount = spark.sql("select * from countCondition").head().getLong(0);
    assertEquals(71, conditionsCount);
  }

  @Test
  void testBuildContextWithTerminologyDefaults() {
    final String terminologyServerUrl = "https://tx.ontoserver.csiro.au/fhir";

    final TerminologyConfiguration terminologyConfig = TerminologyConfiguration.builder()
        .serverUrl(terminologyServerUrl)
        .build();
    final PathlingContext pathlingContext = PathlingContext.create(spark, terminologyConfig);
    assertNotNull(pathlingContext);
    final DefaultTerminologyServiceFactory expectedFactory = new DefaultTerminologyServiceFactory(
        FhirVersionEnum.R4, terminologyConfig);

    final TerminologyServiceFactory actualServiceFactory = pathlingContext.getTerminologyServiceFactory();
    assertEquals(expectedFactory, actualServiceFactory);
    final TerminologyService actualService = actualServiceFactory.build();
    assertNotNull(actualService);
  }

  @Test
  void testBuildContextWithTerminologyNoCache() {
    final String terminologyServerUrl = "https://tx.ontoserver.csiro.au/fhir";

    final HttpClientCachingConfiguration cacheConfig = HttpClientCachingConfiguration.builder()
        .enabled(false)
        .build();
    final TerminologyConfiguration terminologyConfig = TerminologyConfiguration.builder()
        .serverUrl(terminologyServerUrl)
        .cache(cacheConfig)
        .build();
    final PathlingContext pathlingContext = PathlingContext.create(spark, terminologyConfig);
    assertNotNull(pathlingContext);
    final TerminologyServiceFactory expectedFactory = new DefaultTerminologyServiceFactory(
        FhirVersionEnum.R4, terminologyConfig);

    final TerminologyServiceFactory actualServiceFactory = pathlingContext.getTerminologyServiceFactory();
    assertEquals(expectedFactory, actualServiceFactory);
    final TerminologyService actualService = actualServiceFactory.build();
    assertNotNull(actualService);
  }

  @Test
  void testBuildContextWithCustomizedTerminology() throws IOException {
    final String terminologyServerUrl = "https://r4.ontoserver.csiro.au/fhir";
    final String tokenEndpoint = "https://auth.ontoserver.csiro.au/auth/realms/aehrc/protocol/openid-connect/token";
    final String clientId = "some-client";
    final String clientSecret = "some-secret";
    final String scope = "openid";
    final long tokenExpiryTolerance = 300L;

    final int maxConnectionsTotal = 66;
    final int maxConnectionsPerRoute = 33;
    final int socketTimeout = 123;

    final int cacheMaxEntries = 1233;
    final HttpClientCachingStorageType cacheStorageType = HttpClientCachingStorageType.DISK;
    final File tempDirectory = Files.createTempDirectory("pathling-cache").toFile();
    tempDirectory.deleteOnExit();
    final String cacheStoragePath = tempDirectory.getAbsolutePath();

    final HttpClientConfiguration clientConfig = HttpClientConfiguration.builder()
        .maxConnectionsTotal(maxConnectionsTotal)
        .maxConnectionsPerRoute(maxConnectionsPerRoute)
        .socketTimeout(socketTimeout)
        .build();
    final HttpClientCachingConfiguration cacheConfig = HttpClientCachingConfiguration.builder()
        .maxEntries(cacheMaxEntries)
        .storageType(cacheStorageType)
        .storagePath(cacheStoragePath)
        .build();
    final TerminologyAuthConfiguration authConfig = TerminologyAuthConfiguration.builder()
        .tokenEndpoint(tokenEndpoint)
        .clientId(clientId)
        .clientSecret(clientSecret)
        .scope(scope)
        .tokenExpiryTolerance(tokenExpiryTolerance)
        .build();
    final TerminologyConfiguration terminologyConfig = TerminologyConfiguration.builder()
        .serverUrl(terminologyServerUrl)
        .verboseLogging(true)
        .client(clientConfig)
        .cache(cacheConfig)
        .authentication(authConfig)
        .build();

    final PathlingContext pathlingContext = PathlingContext.create(spark, terminologyConfig);
    assertNotNull(pathlingContext);
    final TerminologyServiceFactory expectedFactory = new DefaultTerminologyServiceFactory(
        FhirVersionEnum.R4, terminologyConfig);

    final TerminologyServiceFactory actualServiceFactory = pathlingContext.getTerminologyServiceFactory();
    assertEquals(expectedFactory, actualServiceFactory);
    final TerminologyService actualService = actualServiceFactory.build();
    assertNotNull(actualService);
  }

  @Test
  void failsOnInvalidTerminologyConfiguration() {

    final TerminologyConfiguration invalidTerminologyConfig = TerminologyConfiguration.builder()
        .serverUrl("not-a-URL")
        .client(null)
        .cache(HttpClientCachingConfiguration.builder()
            .storageType(HttpClientCachingStorageType.DISK)
            .build())
        .build();

    final ConstraintViolationException ex = assertThrows(ConstraintViolationException.class,
        () -> PathlingContext.create(spark, invalidTerminologyConfig));

    assertEquals("Invalid terminology configuration:"
        + " cache: If the storage type is disk, then a storage path must be supplied.,"
        + " client: must not be null,"
        + " serverUrl: must be a valid URL", ex.getMessage());
  }

  @Test
  void failsOnInvalidEncodingConfiguration() {

    final TerminologyConfiguration terminologyConfig = TerminologyConfiguration.builder()
        .build();

    final EncodingConfiguration invalidEncodersConfiguration = EncodingConfiguration.builder()
        .maxNestingLevel(-10)
        .openTypes(null)
        .build();

    final ConstraintViolationException ex = assertThrows(ConstraintViolationException.class,
        () -> PathlingContext.create(spark, invalidEncodersConfiguration, terminologyConfig));

    assertEquals("Invalid encoding configuration:"
            + " maxNestingLevel: must be greater than or equal to 0,"
            + " openTypes: must not be null",
        ex.getMessage());
  }

}
