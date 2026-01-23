/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.views;

import static au.csiro.pathling.UnitTestDependencies.fhirContext;
import static au.csiro.pathling.UnitTestDependencies.jsonParser;
import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.validation.ValidationUtils.ensureValid;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static scala.jdk.javaapi.CollectionConverters.asScala;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.datatypes.DecimalCustomCoder;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.utilities.Streams;
import ca.uhn.fhir.context.FhirContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StringType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
@Slf4j
@Import(FhirViewTest.CustomEncoderConfiguration.class)
abstract class FhirViewTest {

  public static final String PATHLING_VIEWS_TEST_DISABLE_EXCLUSIONS = "au.csiro.pathling.views.test.disableExclusions";

  /**
   * Custom configuration for FhirViewTest to provide FhirEncoders with increased maxNestingLevel.
   * This is necessary to support the repeat directive which can create deeply nested structures.
   */
  @TestConfiguration
  static class CustomEncoderConfiguration {

    /**
     * Provides FhirEncoders configured with maxNestingLevel=3 to handle nested structures created
     * by the repeat directive.
     *
     * @return configured FhirEncoders instance
     */
    @Bean
    @Nonnull
    FhirEncoders fhirEncoders() {
      return FhirEncoders.forR4()
          .withMaxNestingLevel(3)
          .withExtensionsEnabled(true)
          .withAllOpenTypes()
          .getOrCreate();
    }
  }

  private static final DateTimeFormatter FHIR_DATE_PARSER = new DateTimeFormatterBuilder()
      .appendPattern("yyyy[-MM[-dd]]")
      .parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
      .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
      .toFormatter();

  private static final DateTimeFormatter FHIR_TIME_FORMATTER = new DateTimeFormatterBuilder()
      .appendPattern("HH:mm:ss")
      .toFormatter();

  private static final UserDefinedFunction LOW_BOUNDARY_FOR_DATE_TIME_UDF = functions.udf(
      (UDF1<String, String>) FhirViewTest::lowBoundaryForDate,
      DataTypes.StringType
  );

  private static final UserDefinedFunction LOW_BOUNDARY_FOR_TIME_UDF = functions.udf(
      (UDF1<String, String>) FhirViewTest::lowBoundaryForTime,
      DataTypes.StringType
  );

  static Path tempDir;

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  @Autowired
  FhirEncoders fhirEncoders;

  @Autowired
  Gson gson;

  @Nonnull
  private final String testLocationGlob;

  @Nonnull
  private final Set<String> includeTags;

  @Nonnull
  private final Set<String> excludedTests;

  @FunctionalInterface
  interface Expectation {

    void expect(@Nonnull final Supplier<Dataset<Row>> result);
  }

  record CompositeExpectation(List<Expectation> expectations) implements Expectation {

    @Override
    public void expect(@Nonnull final Supplier<Dataset<Row>> result) {
      expectations.forEach(expectation -> expectation.expect(result));
    }

  }

  interface ResultExpectation extends Expectation {

    @Override
    default void expect(@Nonnull final Supplier<Dataset<Row>> result) {
      expectResult(result.get());
    }

    void expectResult(@Nonnull final Dataset<Row> rowDataset);

  }

  static class ExpectError implements Expectation {

    @Override
    public void expect(@Nonnull final Supplier<Dataset<Row>> result) {
      assertThrows(Exception.class, () -> result.get().collectAsList());
    }

  }

  @Value
  class Expect implements ResultExpectation {

    public static final String FHIR_DATE_TIME_PATTERN = "^([0-9]([0-9]([0-9][1-9]|[1-9]0)|[1-9]00)|"
        + "[1-9]000)(-(0[1-9]|1[0-2])(-(0[1-9]|[1-2][0-9]|3[0-1])(T([01][0-9]|2[0-3]):[0-5][0-9]:"
        + "([0-5][0-9]|60)(\\.[0-9]{1,9})?)?)?(Z|(\\+|-)((0[0-9]|1[0-3]):[0-5][0-9]|14:00)?)?)?$";
    public static final String FHIR_TIME_PATTERN = "^([01][0-9]|2[0-3]):[0-5][0-9]:([0-5][0-9]|60)"
        + "(\\.[0-9]+)?$";
    Path expectedJson;
    List<String> expectedColumns;

    @Override
    public void expectResult(@Nonnull final Dataset<Row> rowDataset) {
      // Read the expected JSON with prefersDecimal option set to true.
      final Dataset<Row> expectedResult = spark.read()
          .schema(rowDataset.schema())
          .option("prefersDecimal", "true")
          .json(expectedJson.toString());

      // Dynamically create column expressions based on the schema.
      final List<Column> selectColumns = Arrays.stream(expectedResult.schema().fields())
          .map(field -> {
            // All numeric types are cast to decimal to enable consistent comparison.
            if (field.dataType() instanceof DecimalType
                || DataTypes.IntegerType.equals(field.dataType())
                || DataTypes.LongType.equals(field.dataType())
                || DataTypes.DoubleType.equals(field.dataType())) {
              // Use DecimalCustomCoder.decimalType() for the cast type.
              return col(field.name()).cast(DecimalCustomCoder.decimalType()).alias(field.name());
            } else if (field.dataType() instanceof StringType) {
              // Normalize anything that looks like a datetime or time, otherwise pass it through 
              // unaltered.

              return when(
                  col(field.name()).rlike(FHIR_DATE_TIME_PATTERN),
                  LOW_BOUNDARY_FOR_DATE_TIME_UDF.apply(col(field.name()))
              ).when(
                  col(field.name()).rlike(FHIR_TIME_PATTERN),
                  LOW_BOUNDARY_FOR_TIME_UDF.apply(col(field.name()))
              ).otherwise(col(field.name())).alias(field.name());
            } else {
              // Add the field to the selection without alteration.
              return col(field.name());
            }
          }).toList();

      // Select the data with the dynamically created column expressions.
      final Dataset<Row> selectedExpectedResult = expectedResult.select(
          asScala(selectColumns).toSeq());
      final Dataset<Row> selectedActualResult = rowDataset.select(
          asScala(selectColumns).toSeq());

      log.debug("Actual schema:\n {}", selectedActualResult.schema().treeString());

      // Assert that the rowDataset has rows unordered as in selectedExpectedResult.
      assertThat(selectedActualResult).hasRowsAndColumnsUnordered(selectedExpectedResult);
    }

  }

  record ExpectCount(long count) implements ResultExpectation {

    @Override
    public void expectResult(@Nonnull final Dataset<Row> rowDataset) {
      assertEquals(count, rowDataset.count());
    }
  }

  record ExpectColumns(List<String> columns) implements ResultExpectation {

    @Override
    public void expectResult(@Nonnull final Dataset<Row> rowDataset) {
      assertArrayEquals(rowDataset.columns(), columns.toArray());
    }

  }


  protected FhirViewTest(@Nonnull final String testLocationGlob,
      @Nonnull final Set<String> includeTags, @Nonnull final Set<String> excludeTests) {
    this.testLocationGlob = testLocationGlob;
    this.includeTags = includeTags;
    this.excludedTests = excludeTests;
  }

  /**
   * Constructor for the FhirViewTest class.
   *
   * @param includeTags the set of tags to include. Empty set means all tags are included.
   */
  protected FhirViewTest(@Nonnull final String testLocationGlob,
      @Nonnull final Set<String> includeTags) {
    this(testLocationGlob, includeTags, Collections.emptySet());
  }

  protected FhirViewTest(@Nonnull final String testLocationGlob) {
    this(testLocationGlob, Collections.emptySet());
  }


  @BeforeAll
  static void beforeAll() throws IOException {
    tempDir = Files.createTempDirectory("pathling-fhir-view-test");
    log.debug("Created temporary directory: {}", tempDir);
  }

  @Nonnull
  Stream<TestParameters> requests() throws IOException {
    final ObjectMapper mapper = new ObjectMapper();
    final ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
    final Resource[] resources = resolver.getResources(testLocationGlob);
    return Stream.of(resources)
        // Get each test file.
        .map(resource -> {
          try {
            return resource.getFile();
          } catch (final IOException e) {
            throw new RuntimeException(e);
          }
        })
        // Map it to a path.
        .map(File::toPath)
        // Parse the JSON.
        .map(path -> {
          try {
            return mapper.readTree(new FileReader(path.toFile()));
          } catch (final IOException e) {
            throw new RuntimeException(e);
          }
        })
        // Create a TestParameters object for each test within the file.
        .flatMap(testDefinition -> {
          final DataSource sourceData = getDataSource(testDefinition);
          return toTestParameters(testDefinition, sourceData)
              .stream();
        }).filter(this::includeTest);
  }

  boolean includeTest(@Nonnull final TestParameters testParameters) {
    if (this.excludedTests.contains(testParameters.getTitle())) {
      if (System.getProperty(PATHLING_VIEWS_TEST_DISABLE_EXCLUSIONS) != null) {
        log.warn("Ignoring exclusion for test: {}", testParameters);
        return true;
      } else {
        log.warn("Excluding test: {}", testParameters);
        return false;
      }
    } else {
      return true;
    }
  }


  DataSource getDataSource(@Nonnull final JsonNode testDefinition) {
    // Create a parent directory based upon the test name.
    final JsonNode resources = testDefinition.get("resources");
    final TestDataSource result = new TestDataSource();

    // For each resource type, create a dataset and add it to the result.
    Streams.streamOf(resources.elements())
        // groupBy resource type and convert the value using toString()
        .collect(Collectors.groupingBy(
            resource -> resource.get("resourceType").asText(),
            mapping(Object::toString, toList()
            ))
        ).forEach((resourceType, jsonStrings) -> {
          final Dataset<String> dataset = spark.createDataset(jsonStrings, Encoders.STRING());
          final ExpressionEncoder<IBaseResource> encoder = fhirEncoders.of(resourceType);
          final Dataset<Row> resourceDataset = dataset.map(
              (MapFunction<String, IBaseResource>) json -> jsonParser(fhirContext())
                  .parseResource(json), encoder).toDF().cache();
          result.put(resourceType, resourceDataset);
        });
    return result;
  }

  List<TestParameters> toTestParameters(@Nonnull final JsonNode testDefinition,
      @Nonnull final DataSource sourceData) {
    try {
      final JsonNode views = testDefinition.get("tests");
      final List<TestParameters> result = new ArrayList<>();

      int testNumber = 0;
      for (final Iterator<JsonNode> it = views.elements(); it.hasNext(); ) {
        final JsonNode view = it.next();

        final List<String> tags = Optional.ofNullable(view.get("tags"))
            .map(JsonNode::elements)
            .map(Streams::streamOf)
            .orElse(Stream.empty())
            .map(JsonNode::asText)
            .toList();

        if (includeTags.isEmpty() || !Collections.disjoint(tags, includeTags)) {
          // Get the view JSON.
          final String viewJson = view.get("view").toPrettyString();

          // Write the expected JSON to a file, named after the view.
          final Path directory = getTempDir(testDefinition);
          final String expectedFileName =
              String.format("%02d_%s.json", testNumber,
                  view.get("title").asText().replaceAll("\\W+", "_"));
          final Path expectedPath = directory.resolve(expectedFileName);
          final boolean disabled = Optional.ofNullable(view.get("disabled"))
              .map(JsonNode::asBoolean).orElse(false);
          result.add(
              new TestParameters(testDefinition.get("title").asText(), view.get("title").asText(),
                  sourceData, viewJson, getExpectation(view, expectedPath),
                  disabled));
          testNumber++;
        }
      }
      return result;
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }


  @Nonnull
  Expectation getExpectation(@Nonnull final JsonNode testDefinition,
      @Nonnull final Path expectedPath)
      throws IOException {
    @Nullable final JsonNode expectation;
    if (nonNull(testDefinition.get("expectError"))) {
      return new ExpectError();
    } else if (nonNull(expectation = testDefinition.get("expectCount"))) {
      return new ExpectCount(expectation.asLong());
    } else if (testDefinition.has("expect") || testDefinition.has("expectColumns")) {
      final JsonNode expect = testDefinition.get("expect");
      final JsonNode expectColumns = testDefinition.get("expectColumns");
      final List<Expectation> expectations = new ArrayList<>();
      if (expect != null) {
        expectations.add(buildResultExpectation(expectedPath, expect));
      }
      if (expectColumns != null) {
        final List<String> columns = new ArrayList<>();
        expectColumns.elements().forEachRemaining(column -> columns.add(column.asText()));
        expectations.add(new ExpectColumns(columns));
      }
      return new CompositeExpectation(expectations);
    } else {
      log.info("No expectation found for test:");
      log.info(testDefinition.toPrettyString());
      throw new RuntimeException("No expectation found");
    }
  }

  @Nonnull
  private Expect buildResultExpectation(final @Nonnull Path expectedPath,
      final @Nonnull JsonNode expectation)
      throws IOException {
    List<String> expectedColumns = null;
    Files.createFile(expectedPath);
    for (final Iterator<JsonNode> rowIt = expectation.elements(); rowIt.hasNext(); ) {
      final JsonNode row = rowIt.next();
      // Get the columns from the first row.
      if (expectedColumns == null) {
        final List<String> columns = new ArrayList<>();
        row.fields().forEachRemaining(field -> columns.add(field.getKey()));
        expectedColumns = columns;
      }
      // Append the row to the file.
      Files.writeString(expectedPath, row + "\n",
          StandardOpenOption.APPEND);
    }
    return new Expect(expectedPath, expectedColumns != null
                                    ? expectedColumns
                                    : Collections.emptyList());
  }

  @Nonnull
  private static Path getTempDir(final @Nonnull JsonNode testDefinition) throws IOException {
    final String directoryName = testDefinition.get("title").asText().replaceAll("\\W+", "_");
    final Path directory = tempDir.resolve(directoryName);
    try {
      return Files.createDirectory(directory);
    } catch (final FileAlreadyExistsException ignored) {
      return directory;
    }
  }

  @ParameterizedTest
  @MethodSource("requests")
  void test(@Nonnull final TestParameters parameters) {
    assumeFalse(parameters.disabled(), "Test is disabled");
    log.info("Running test: {}", parameters.getTitle());

    parameters.expectation().expect(() -> {
      final FhirView view;
      try {
        // Serialize a FhirView object from the view definition in the test.
        view = gson.fromJson(parameters.viewJson(), FhirView.class);
        ensureValid(view, "View is not valid");
      } catch (final Exception e) {
        // If parsing the view definition fails, log the JSON and rethrow the exception.
        log.info("Exception occurred while parsing test definition - {}", e.getMessage());
        log.info(parameters.viewJson());
        throw e;
      }

      // Create a new executor and build the query.
      final FhirViewExecutor executor = new FhirViewExecutor(fhirContext,
          parameters.sourceData());
      return executor.buildQuery(view);
    });
  }

  record TestParameters(
      @Nonnull String suiteName,
      @Nonnull String testName,
      @Nonnull DataSource sourceData,
      @Nonnull String viewJson,
      @Nonnull Expectation expectation,
      boolean disabled
  ) {

    @Nonnull
    public String getTitle() {
      return suiteName + " - " + testName;
    }

    @Nonnull
    @Override
    public String toString() {
      return getTitle();
    }
  }

  /**
   * A class for making FHIR data available for the view tests.
   *
   * @author John Grimes
   */
  @Slf4j
  public static class TestDataSource implements DataSource {

    private static final Map<String, Dataset<Row>> resourceTypeToDataset = new HashMap<>();

    public void put(@Nonnull final String resourceType, @Nonnull final Dataset<Row> dataset) {
      resourceTypeToDataset.put(resourceType, dataset);
    }

    @Nonnull
    @Override
    public Dataset<Row> read(@Nullable final String resourceCode) {
      return requireNonNull(resourceTypeToDataset.get(resourceCode));
    }

    @Override
    public @Nonnull Set<String> getResourceTypes() {
      return resourceTypeToDataset.keySet();
    }

  }

  @Nullable
  private static String lowBoundaryForDate(@Nullable final String dateString) {
    if (dateString == null) {
      return null;
    }
    try {
      final TemporalAccessor temporalAccessor = FHIR_DATE_PARSER.parse(dateString);
      return DateTimeFormatter.ISO_LOCAL_DATE.format(LocalDate.from(temporalAccessor));
    } catch (final DateTimeParseException e) {
      // Fallback for invalid date strings.
      return null;
    }
  }

  @Nullable
  private static String lowBoundaryForTime(@Nullable final String timeString) {
    if (timeString == null) {
      return null;
    }
    try {
      final LocalTime localTime = LocalTime.parse(timeString);
      return FHIR_TIME_FORMATTER.format(localTime);
    } catch (final DateTimeParseException e) {
      // Fallback for invalid time strings.
      return null;
    }
  }

}
