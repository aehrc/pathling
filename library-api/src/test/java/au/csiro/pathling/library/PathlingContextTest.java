/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.config.TerminologyAuthConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.search.UnknownSearchParameterException;
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
import scala.collection.mutable.ArraySeq;

@Slf4j
public class PathlingContextTest {

  private static SparkSession spark;
  private static final String TEST_DATA_URL = "target/encoders-tests/data";

  /** Set up Spark. */
  @BeforeAll
  static void setUpAll() {
    spark = TestHelpers.spark();
  }

  /** Tear down Spark. */
  @AfterAll
  static void tearDownAll() {
    spark.stop();
  }

  private TerminologyServiceFactory terminologyServiceFactory;
  private TerminologyService terminologyService;

  private static final String GUID_REG_SUBEXPRESSION =
      "[0-9a-fA-F]{8}" + "-([0-9a-fA-F]{4}-)" + "{3}[0-9a-fA-F]{12}";

  public static final Pattern GUID_REGEX = Pattern.compile("^" + GUID_REG_SUBEXPRESSION + "$");

  private static final Pattern RELATIVE_REF_REGEX =
      Pattern.compile("^[A-Z][A-Za-z]+/" + GUID_REG_SUBEXPRESSION + "$");

  public static boolean isValidGUID(@Nonnull final String maybeGUID) {
    final Matcher m = GUID_REGEX.matcher(maybeGUID);
    return m.matches();
  }

  public static boolean isValidRelativeReference(@Nonnull final String maybeRelativeRef) {
    final Matcher m = RELATIVE_REF_REGEX.matcher(maybeRelativeRef);
    return m.matches();
  }

  public void assertAllGUIDs(@Nonnull final Collection<String> maybeGuids) {
    final List<String> invalidValues =
        maybeGuids.stream().filter(not(PathlingContextTest::isValidGUID)).limit(7).toList();
    assertTrue(
        invalidValues.isEmpty(), "All values should be GUIDs, but some are not: " + invalidValues);
  }

  public void assertAllRelativeReferences(@Nonnull final Collection<String> maybeRelativeRefs) {
    final List<String> invalidValues =
        maybeRelativeRefs.stream()
            .filter(not(PathlingContextTest::isValidRelativeReference))
            .limit(7)
            .toList();
    assertTrue(
        invalidValues.isEmpty(),
        "All values should be relative references, but some are not: " + invalidValues);
  }

  public <T> void assertValidIdColumns(@Nonnull final Dataset<T> df) {
    assertAllGUIDs(df.select("id").as(Encoders.STRING()).collectAsList());
    assertAllRelativeReferences(df.select("id_versioned").as(Encoders.STRING()).collectAsList());
  }

  public <T> void assertValidRelativeRefColumns(
      @Nonnull final Dataset<T> df, final Column... refColumns) {
    Stream.of(refColumns)
        .forEach(
            c ->
                assertAllRelativeReferences(
                    df.select(c.getField("reference")).as(Encoders.STRING()).collectAsList()));
  }

  @BeforeEach
  void setUp() {
    // setup terminology mocks
    terminologyServiceFactory =
        mock(TerminologyServiceFactory.class, withSettings().serializable());
    terminologyService = mock(TerminologyService.class, withSettings().serializable());
    when(terminologyServiceFactory.build()).thenReturn(terminologyService);

    DefaultTerminologyServiceFactory.reset();
  }

  @Test
  void testEncodeResourcesFromJsonBundle() {

    final Dataset<String> bundlesDF =
        spark.read().option("wholetext", true).textFile(TEST_DATA_URL + "/bundles/R4/json");

    final PathlingContext pathling = PathlingContext.create(spark);

    final Dataset<Row> patientsDataframe =
        pathling.encodeBundle(bundlesDF.toDF(), "Patient", PathlingContext.FHIR_JSON);
    assertEquals(5, patientsDataframe.count());

    assertValidIdColumns(patientsDataframe);

    // Test omission of MIME type.
    final Dataset<Row> patientsDataframe2 = pathling.encodeBundle(bundlesDF.toDF(), "Patient");
    assertEquals(5, patientsDataframe2.count());
    assertValidIdColumns(patientsDataframe2);

    final Dataset<Condition> conditionsDataframe =
        pathling.encodeBundle(bundlesDF, Condition.class, PathlingContext.FHIR_JSON);
    assertEquals(107, conditionsDataframe.count());

    assertValidIdColumns(conditionsDataframe);
    assertValidRelativeRefColumns(conditionsDataframe, col("subject"));
    assertValidRelativeRefColumns(conditionsDataframe, col("encounter"));
  }

  @Test
  void testEncodeResourcesFromXmlBundle() {
    final Dataset<String> bundlesDF =
        spark.read().option("wholetext", true).textFile(TEST_DATA_URL + "/bundles/R4/xml");

    final PathlingContext pathling = PathlingContext.create(spark);
    final Dataset<Condition> conditionsDataframe =
        pathling.encodeBundle(bundlesDF, Condition.class, PathlingContext.FHIR_XML);
    assertEquals(107, conditionsDataframe.count());

    assertValidIdColumns(conditionsDataframe);
    assertValidRelativeRefColumns(conditionsDataframe, col("subject"));
    assertValidRelativeRefColumns(conditionsDataframe, col("encounter"));
  }

  @Test
  void testEncodeResourcesFromJson() {
    final Dataset<String> jsonResources =
        spark.read().textFile(TEST_DATA_URL + "/resources/R4/json");

    final PathlingContext pathling = PathlingContext.create(spark);

    final Dataset<Row> patientsDataframe =
        pathling.encode(jsonResources.toDF(), "Patient", PathlingContext.FHIR_JSON);
    assertEquals(9, patientsDataframe.count());
    assertValidIdColumns(patientsDataframe);

    final Dataset<Condition> conditionsDataframe =
        pathling.encode(jsonResources, Condition.class, PathlingContext.FHIR_JSON);
    assertEquals(71, conditionsDataframe.count());

    assertValidIdColumns(conditionsDataframe);
    assertValidRelativeRefColumns(conditionsDataframe, col("subject"));
    assertValidRelativeRefColumns(conditionsDataframe, col("encounter"));
  }

  @Test
  void testEncoderOptions() {
    final Dataset<Row> jsonResourcesDF = spark.read().text(TEST_DATA_URL + "/resources/R4/json");

    // Test the defaults
    final Row defaultRow =
        PathlingContext.create(spark).encode(jsonResourcesDF, "Questionnaire").head();
    assertFieldPresent("_extension", defaultRow.schema());
    final Row defaultItem = (Row) defaultRow.getList(defaultRow.fieldIndex("item")).getFirst();
    assertFieldPresent("item", defaultItem.schema());

    // Test explicit options
    // Nested items
    final EncodingConfiguration encodingConfig1 =
        EncodingConfiguration.builder().enableExtensions(false).maxNestingLevel(1).build();
    final PathlingContext context1 = PathlingContext.createForEncoding(spark, encodingConfig1);

    final Row rowWithNesting = context1.encode(jsonResourcesDF, "Questionnaire").head();
    assertFieldNotPresent("_extension", rowWithNesting.schema());
    // Test item nesting
    final Row itemWithNesting =
        (Row) rowWithNesting.getList(rowWithNesting.fieldIndex("item")).getFirst();
    assertFieldPresent("item", itemWithNesting.schema());
    final Row nestedItem =
        (Row) itemWithNesting.getList(itemWithNesting.fieldIndex("item")).getFirst();
    assertFieldNotPresent("item", nestedItem.schema());

    // Verify encoding configuration can be retrieved
    final EncodingConfiguration retrievedConfig1 = context1.getEncodingConfiguration();
    assertFalse(retrievedConfig1.isEnableExtensions());
    assertEquals(1, retrievedConfig1.getMaxNestingLevel());

    // Test explicit options
    // Extensions and open types
    final EncodingConfiguration encodingConfig2 =
        EncodingConfiguration.builder()
            .enableExtensions(true)
            .openTypes(Set.of("boolean", "string", "Address"))
            .build();
    final PathlingContext context2 = PathlingContext.createForEncoding(spark, encodingConfig2);
    final Row rowWithExtensions = context2.encode(jsonResourcesDF, "Patient").head();
    assertFieldPresent("_extension", rowWithExtensions.schema());

    final Map<Integer, ArraySeq<Row>> extensions =
        rowWithExtensions.getJavaMap(rowWithExtensions.fieldIndex("_extension"));

    // get the first extension of some extension set
    final Row extension = (Row) extensions.values().toArray(ArraySeq[]::new)[0].apply(0);
    assertFieldPresent("valueString", extension.schema());
    assertFieldPresent("valueAddress", extension.schema());
    assertFieldPresent("valueBoolean", extension.schema());
    assertFieldNotPresent("valueInteger", extension.schema());

    // Verify encoding configuration can be retrieved
    final EncodingConfiguration retrievedConfig2 = context2.getEncodingConfiguration();
    assertTrue(retrievedConfig2.isEnableExtensions());
    assertEquals(Set.of("boolean", "string", "Address"), retrievedConfig2.getOpenTypes());
  }

  @Test
  void testEncodeResourceStream() throws Exception {
    final EncodingConfiguration encodingConfig =
        EncodingConfiguration.builder().enableExtensions(true).build();
    final PathlingContext pathling = PathlingContext.createForEncoding(spark, encodingConfig);

    final Dataset<Row> jsonResources =
        spark.readStream().text(TEST_DATA_URL + "/resources/R4/json");

    assertTrue(jsonResources.isStreaming());

    final Dataset<Row> patientsStream =
        pathling.encode(jsonResources, "Patient", PathlingContext.FHIR_JSON);

    assertTrue(patientsStream.isStreaming());

    final StreamingQuery patientsQuery =
        patientsStream.writeStream().queryName("patients").format("memory").start();

    patientsQuery.processAllAvailable();
    final long patientsCount = spark.sql("select count(*) from patients").head().getLong(0);
    assertEquals(9, patientsCount);

    final StreamingQuery conditionQuery =
        pathling
            .encode(jsonResources, "Condition", PathlingContext.FHIR_JSON)
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

    final TerminologyConfiguration terminologyConfig =
        TerminologyConfiguration.builder().serverUrl(terminologyServerUrl).build();
    final PathlingContext pathlingContext =
        PathlingContext.createForTerminology(spark, terminologyConfig);

    assertNotNull(pathlingContext);
    final DefaultTerminologyServiceFactory expectedFactory =
        new DefaultTerminologyServiceFactory(FhirVersionEnum.R4, terminologyConfig);

    final TerminologyServiceFactory actualServiceFactory =
        pathlingContext.getTerminologyServiceFactory();
    assertEquals(expectedFactory, actualServiceFactory);
    final TerminologyService actualService = actualServiceFactory.build();
    assertNotNull(actualService);

    // Verify terminology configuration can be retrieved
    final TerminologyConfiguration retrievedConfig = pathlingContext.getTerminologyConfiguration();
    assertNotNull(retrievedConfig);
    assertEquals(terminologyServerUrl, retrievedConfig.getServerUrl());
    assertTrue(retrievedConfig.isEnabled());
  }

  @Test
  void testBuildContextWithTerminologyNoCache() {
    final String terminologyServerUrl = "https://tx.ontoserver.csiro.au/fhir";

    final HttpClientCachingConfiguration cacheConfig =
        HttpClientCachingConfiguration.builder().enabled(false).build();
    final TerminologyConfiguration terminologyConfig =
        TerminologyConfiguration.builder()
            .serverUrl(terminologyServerUrl)
            .cache(cacheConfig)
            .build();
    final PathlingContext pathlingContext =
        PathlingContext.createForTerminology(spark, terminologyConfig);
    assertNotNull(pathlingContext);
    final TerminologyServiceFactory expectedFactory =
        new DefaultTerminologyServiceFactory(FhirVersionEnum.R4, terminologyConfig);

    final TerminologyServiceFactory actualServiceFactory =
        pathlingContext.getTerminologyServiceFactory();
    assertEquals(expectedFactory, actualServiceFactory);
    final TerminologyService actualService = actualServiceFactory.build();
    assertNotNull(actualService);
  }

  @Test
  void testBuildContextWithCustomizedTerminology() throws IOException {
    final String terminologyServerUrl = "https://r4.ontoserver.csiro.au/fhir";
    final String tokenEndpoint =
        "https://auth.ontoserver.csiro.au/auth/realms/aehrc/protocol/openid-connect/token";
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

    final HttpClientConfiguration clientConfig =
        HttpClientConfiguration.builder()
            .maxConnectionsTotal(maxConnectionsTotal)
            .maxConnectionsPerRoute(maxConnectionsPerRoute)
            .socketTimeout(socketTimeout)
            .build();
    final HttpClientCachingConfiguration cacheConfig =
        HttpClientCachingConfiguration.builder()
            .maxEntries(cacheMaxEntries)
            .storageType(cacheStorageType)
            .storagePath(cacheStoragePath)
            .build();
    final TerminologyAuthConfiguration authConfig =
        TerminologyAuthConfiguration.builder()
            .tokenEndpoint(tokenEndpoint)
            .clientId(clientId)
            .clientSecret(clientSecret)
            .scope(scope)
            .tokenExpiryTolerance(tokenExpiryTolerance)
            .build();
    final TerminologyConfiguration terminologyConfig =
        TerminologyConfiguration.builder()
            .serverUrl(terminologyServerUrl)
            .verboseLogging(true)
            .client(clientConfig)
            .cache(cacheConfig)
            .authentication(authConfig)
            .build();

    final PathlingContext pathlingContext =
        PathlingContext.createForTerminology(spark, terminologyConfig);

    assertNotNull(pathlingContext);
    final TerminologyServiceFactory expectedFactory =
        new DefaultTerminologyServiceFactory(FhirVersionEnum.R4, terminologyConfig);

    final TerminologyServiceFactory actualServiceFactory =
        pathlingContext.getTerminologyServiceFactory();
    assertEquals(expectedFactory, actualServiceFactory);
    final TerminologyService actualService = actualServiceFactory.build();
    assertNotNull(actualService);

    // Verify terminology configuration can be retrieved with all custom values
    final TerminologyConfiguration retrievedConfig = pathlingContext.getTerminologyConfiguration();
    assertNotNull(retrievedConfig);
    assertEquals(terminologyServerUrl, retrievedConfig.getServerUrl());
    assertTrue(retrievedConfig.isVerboseLogging());
    assertEquals(maxConnectionsTotal, retrievedConfig.getClient().getMaxConnectionsTotal());
    assertEquals(maxConnectionsPerRoute, retrievedConfig.getClient().getMaxConnectionsPerRoute());
    assertEquals(socketTimeout, retrievedConfig.getClient().getSocketTimeout());
    assertEquals(cacheMaxEntries, retrievedConfig.getCache().getMaxEntries());
    assertEquals(cacheStorageType, retrievedConfig.getCache().getStorageType());
    assertEquals(cacheStoragePath, retrievedConfig.getCache().getStoragePath());
    assertEquals(tokenEndpoint, retrievedConfig.getAuthentication().getTokenEndpoint());
    assertEquals(clientId, retrievedConfig.getAuthentication().getClientId());
    assertEquals(clientSecret, retrievedConfig.getAuthentication().getClientSecret());
    assertEquals(scope, retrievedConfig.getAuthentication().getScope());
    assertEquals(
        tokenExpiryTolerance, retrievedConfig.getAuthentication().getTokenExpiryTolerance());
  }

  @Test
  void failsOnInvalidTerminologyConfiguration() {

    final TerminologyConfiguration invalidTerminologyConfig =
        TerminologyConfiguration.builder()
            .serverUrl("not-a-URL")
            .client(null)
            .cache(
                HttpClientCachingConfiguration.builder()
                    .storageType(HttpClientCachingStorageType.DISK)
                    .build())
            .build();

    final PathlingContext.Builder builder =
        PathlingContext.builder(spark).terminologyConfiguration(invalidTerminologyConfig);

    final ConstraintViolationException ex =
        assertThrows(ConstraintViolationException.class, builder::build);

    assertEquals(
        "Invalid terminology configuration:"
            + " cache: If the storage type is disk, then a storage path must be supplied.,"
            + " client: must not be null,"
            + " serverUrl: must be a valid URL",
        ex.getMessage());
  }

  @Test
  void failsOnInvalidEncodingConfiguration() {

    final TerminologyConfiguration terminologyConfig = TerminologyConfiguration.builder().build();

    final EncodingConfiguration invalidEncodersConfiguration =
        EncodingConfiguration.builder().maxNestingLevel(-10).openTypes(null).build();

    final PathlingContext.Builder builder =
        PathlingContext.builder(spark)
            .encodingConfiguration(invalidEncodersConfiguration)
            .terminologyConfiguration(terminologyConfig);

    final ConstraintViolationException ex =
        assertThrows(ConstraintViolationException.class, builder::build);

    assertEquals(
        "Invalid encoding configuration:"
            + " maxNestingLevel: must be greater than or equal to 0,"
            + " openTypes: must not be null",
        ex.getMessage());
  }

  @Test
  void testBuildContextWithQueryConfiguration() {
    final QueryConfiguration queryConfig =
        QueryConfiguration.builder().explainQueries(true).maxUnboundTraversalDepth(20).build();

    final PathlingContext pathlingContext =
        PathlingContext.builder(spark).queryConfiguration(queryConfig).build();

    assertNotNull(pathlingContext);
    assertNotNull(pathlingContext.getQueryConfiguration());
    assertTrue(pathlingContext.getQueryConfiguration().isExplainQueries());
    assertEquals(20, pathlingContext.getQueryConfiguration().getMaxUnboundTraversalDepth());

    // Verify configuration can be retrieved
    final QueryConfiguration retrievedConfig = pathlingContext.getQueryConfiguration();
    assertEquals(queryConfig, retrievedConfig);
  }

  @Test
  void testBuildContextWithDefaultQueryConfiguration() {
    final PathlingContext pathlingContext = PathlingContext.builder(spark).build();

    assertNotNull(pathlingContext);
    assertNotNull(pathlingContext.getQueryConfiguration());
    assertFalse(pathlingContext.getQueryConfiguration().isExplainQueries());
    assertEquals(10, pathlingContext.getQueryConfiguration().getMaxUnboundTraversalDepth());
  }

  @Test
  void failsOnInvalidQueryConfiguration() {
    // Test with negative maxUnboundTraversalDepth
    final QueryConfiguration invalidQueryConfig =
        QueryConfiguration.builder().maxUnboundTraversalDepth(-5).build();

    final PathlingContext.Builder builder =
        PathlingContext.builder(spark).queryConfiguration(invalidQueryConfig);

    final ConstraintViolationException ex =
        assertThrows(ConstraintViolationException.class, builder::build);

    final String message = ex.getMessage();
    assertTrue(
        message.contains("maxUnboundTraversalDepth: must be greater than or equal to 0"),
        "Expected error message to contain 'maxUnboundTraversalDepth: must be greater than or equal"
            + " to 0', but was: "
            + message);
  }

  @Test
  void testBuilderWithNullSpark() {
    final PathlingContext pathlingContext = PathlingContext.builder().spark(null).build();

    assertNotNull(pathlingContext);
    assertNotNull(pathlingContext.getSpark());
  }

  @Test
  void testBuilderWithAllConfigurations() {
    final EncodingConfiguration encodingConfig =
        EncodingConfiguration.builder().maxNestingLevel(5).enableExtensions(false).build();

    final TerminologyConfiguration terminologyConfig = TerminologyConfiguration.builder().build();

    final QueryConfiguration queryConfig =
        QueryConfiguration.builder().explainQueries(true).maxUnboundTraversalDepth(15).build();

    final PathlingContext pathlingContext =
        PathlingContext.builder(spark)
            .encodingConfiguration(encodingConfig)
            .terminologyConfiguration(terminologyConfig)
            .queryConfiguration(queryConfig)
            .build();

    assertNotNull(pathlingContext);
    assertNotNull(pathlingContext.getQueryConfiguration());
    assertTrue(pathlingContext.getQueryConfiguration().isExplainQueries());
    assertEquals(15, pathlingContext.getQueryConfiguration().getMaxUnboundTraversalDepth());
  }

  @Test
  void testConfigurationRoundTrip() {
    // Create specific configurations
    final EncodingConfiguration encodingConfig =
        EncodingConfiguration.builder()
            .maxNestingLevel(7)
            .enableExtensions(true)
            .openTypes(Set.of("string", "Coding"))
            .build();

    final TerminologyConfiguration terminologyConfig =
        TerminologyConfiguration.builder()
            .serverUrl("https://test.server.com/fhir")
            .verboseLogging(true)
            .build();

    final QueryConfiguration queryConfig =
        QueryConfiguration.builder().explainQueries(true).maxUnboundTraversalDepth(25).build();

    // Build context with configurations
    final PathlingContext pathlingContext =
        PathlingContext.builder(spark)
            .encodingConfiguration(encodingConfig)
            .terminologyConfiguration(terminologyConfig)
            .queryConfiguration(queryConfig)
            .build();

    // Retrieve configurations
    final EncodingConfiguration retrievedEncodingConfig =
        pathlingContext.getEncodingConfiguration();
    final TerminologyConfiguration retrievedTerminologyConfig =
        pathlingContext.getTerminologyConfiguration();
    final QueryConfiguration retrievedQueryConfig = pathlingContext.getQueryConfiguration();

    // Verify encoding configuration matches
    assertEquals(7, retrievedEncodingConfig.getMaxNestingLevel());
    assertTrue(retrievedEncodingConfig.isEnableExtensions());
    assertEquals(Set.of("string", "Coding"), retrievedEncodingConfig.getOpenTypes());

    // Verify terminology configuration matches
    assertEquals("https://test.server.com/fhir", retrievedTerminologyConfig.getServerUrl());
    assertTrue(retrievedTerminologyConfig.isVerboseLogging());

    // Verify query configuration matches
    assertEquals(queryConfig, retrievedQueryConfig);
    assertTrue(retrievedQueryConfig.isExplainQueries());
    assertEquals(25, retrievedQueryConfig.getMaxUnboundTraversalDepth());
  }

  @Test
  void testTerminologyConfigurationWithNonConfigurableFactory() {
    // Create a mock terminology service factory that uses default implementation
    final TerminologyServiceFactory mockFactory =
        mock(TerminologyServiceFactory.class, withSettings().serializable());

    // Mock uses default method which throws IllegalStateException
    when(mockFactory.getConfiguration()).thenCallRealMethod();

    // Create context using createInternal (which bypasses builder validations)
    final PathlingContext pathlingContext =
        PathlingContext.createInternal(spark, FhirEncoders.forR4().getOrCreate(), mockFactory);

    // Verify default implementation throws expected exception
    final IllegalStateException ex =
        assertThrows(IllegalStateException.class, pathlingContext::getTerminologyConfiguration);

    assertTrue(
        ex.getMessage().contains("does not support configuration access"),
        "Expected error message to contain 'does not support configuration access', but was: "
            + ex.getMessage());
  }

  // ========== searchToColumn tests ==========

  @Test
  void searchToColumn_singleParameter() {
    final PathlingContext pathling = PathlingContext.create(spark);
    final Column genderFilter = pathling.searchToColumn("Patient", "gender=male");

    assertNotNull(genderFilter);
  }

  @Test
  void searchToColumn_multipleParameters() {
    final PathlingContext pathling = PathlingContext.create(spark);
    final Column combinedFilter = pathling.searchToColumn("Patient", "gender=male&active=true");

    assertNotNull(combinedFilter);
  }

  @Test
  void searchToColumn_datePrefix() {
    final PathlingContext pathling = PathlingContext.create(spark);
    final Column dateFilter = pathling.searchToColumn("Patient", "birthdate=ge1990-01-01");

    assertNotNull(dateFilter);
  }

  @Test
  void searchToColumn_combineWithAnd() {
    final PathlingContext pathling = PathlingContext.create(spark);

    final Column genderFilter = pathling.searchToColumn("Patient", "gender=male");
    final Column activeFilter = pathling.searchToColumn("Patient", "active=true");
    final Column combined = genderFilter.and(activeFilter);

    assertNotNull(combined);
  }

  @Test
  void searchToColumn_combineWithOr() {
    final PathlingContext pathling = PathlingContext.create(spark);

    final Column maleFilter = pathling.searchToColumn("Patient", "gender=male");
    final Column femaleFilter = pathling.searchToColumn("Patient", "gender=female");
    final Column combined = maleFilter.or(femaleFilter);

    assertNotNull(combined);
  }

  @Test
  void searchToColumn_applyToDataFrame() {
    final Dataset<String> bundlesDF =
        spark.read().option("wholetext", true).textFile(TEST_DATA_URL + "/bundles/R4/json");

    final PathlingContext pathling = PathlingContext.create(spark);
    final Dataset<Row> patientsDataframe =
        pathling.encodeBundle(bundlesDF.toDF(), "Patient", PathlingContext.FHIR_JSON);

    // Create filter and apply it
    final Column genderFilter = pathling.searchToColumn("Patient", "gender=male");
    final Dataset<Row> filteredPatients = patientsDataframe.filter(genderFilter);

    // Should return fewer patients than the original dataset
    assertTrue(filteredPatients.count() <= patientsDataframe.count());
  }

  @Test
  void searchToColumn_emptyQueryMatchesAllResources() {
    final Dataset<String> bundlesDF =
        spark.read().option("wholetext", true).textFile(TEST_DATA_URL + "/bundles/R4/json");

    final PathlingContext pathling = PathlingContext.create(spark);
    final Dataset<Row> patientsDataframe =
        pathling.encodeBundle(bundlesDF.toDF(), "Patient", PathlingContext.FHIR_JSON);

    final Column emptyFilter = pathling.searchToColumn("Patient", "");
    final Dataset<Row> filteredPatients = patientsDataframe.filter(emptyFilter);

    // Empty filter should match all resources
    assertEquals(patientsDataframe.count(), filteredPatients.count());
  }

  @Test
  void searchToColumn_invalidParameter_throwsException() {
    final PathlingContext pathling = PathlingContext.create(spark);

    assertThrows(
        UnknownSearchParameterException.class,
        () -> pathling.searchToColumn("Patient", "invalid-param=value"));
  }

  // ========== fhirPathToColumn tests ==========

  @Test
  void fhirPathToColumn_booleanExpression() {
    // A boolean FHIRPath expression should return a non-null Column.
    final PathlingContext pathling = PathlingContext.create(spark);
    final Column genderFilter = pathling.fhirPathToColumn("Patient", "gender = 'male'");

    assertNotNull(genderFilter);
  }

  @Test
  void fhirPathToColumn_valueExpression() {
    // A value FHIRPath expression should return a non-null Column.
    final PathlingContext pathling = PathlingContext.create(spark);
    final Column nameColumn = pathling.fhirPathToColumn("Patient", "name.given.first()");

    assertNotNull(nameColumn);
  }

  @Test
  void fhirPathToColumn_applyToDataFrameFilter() {
    // The returned Column should be usable with DataFrame.filter().
    final Dataset<String> bundlesDF =
        spark.read().option("wholetext", true).textFile(TEST_DATA_URL + "/bundles/R4/json");

    final PathlingContext pathling = PathlingContext.create(spark);
    final Dataset<Row> patientsDataframe =
        pathling.encodeBundle(bundlesDF.toDF(), "Patient", PathlingContext.FHIR_JSON);

    final Column genderFilter = pathling.fhirPathToColumn("Patient", "gender = 'male'");
    final Dataset<Row> filteredPatients = patientsDataframe.filter(genderFilter);

    assertTrue(filteredPatients.count() <= patientsDataframe.count());
  }

  @Test
  void fhirPathToColumn_applyToDataFrameSelect() {
    // The returned Column should be usable with DataFrame.select().
    final Dataset<String> bundlesDF =
        spark.read().option("wholetext", true).textFile(TEST_DATA_URL + "/bundles/R4/json");

    final PathlingContext pathling = PathlingContext.create(spark);
    final Dataset<Row> patientsDataframe =
        pathling.encodeBundle(bundlesDF.toDF(), "Patient", PathlingContext.FHIR_JSON);

    final Column nameColumn = pathling.fhirPathToColumn("Patient", "name.given.first()");
    final Dataset<Row> selectedNames = patientsDataframe.select(nameColumn);

    assertEquals(patientsDataframe.count(), selectedNames.count());
  }

  @Test
  void fhirPathToColumn_invalidExpression_throwsException() {
    // An invalid FHIRPath expression should throw an exception.
    final PathlingContext pathling = PathlingContext.create(spark);

    assertThrows(Exception.class, () -> pathling.fhirPathToColumn("Patient", "!!invalid!!"));
  }

  @Test
  void fhirPathToColumn_invalidResourceType_throwsException() {
    // An invalid resource type should throw an exception.
    final PathlingContext pathling = PathlingContext.create(spark);

    assertThrows(Exception.class, () -> pathling.fhirPathToColumn("InvalidResource", "gender"));
  }
}
