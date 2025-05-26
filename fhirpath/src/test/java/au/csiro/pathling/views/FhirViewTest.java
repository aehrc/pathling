package au.csiro.pathling.views;

import static au.csiro.pathling.UnitTestDependencies.fhirContext;
import static au.csiro.pathling.UnitTestDependencies.jsonParser;
import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.validation.ValidationUtils.ensureValid;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static scala.collection.JavaConversions.asScalaBuffer;

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
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StringType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.utilities.Utilities;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
@Slf4j
abstract class FhirViewTest {


  /**
   * Precision that includes only the year, month and day of a date string.
   */
  protected static final int DATE_BOUNDARY_PRECISION = 8;

  /**
   * Precision that includes all components of a time string.
   */
  protected static final int TIME_BOUNDARY_PRECISION = 9;

  protected static final UserDefinedFunction LOW_BOUNDARY_FOR_DATE_TIME_UDF = functions.udf(
      (String s) -> Utilities.lowBoundaryForDate(s, DATE_BOUNDARY_PRECISION),
      DataTypes.StringType
  );

  protected static final UserDefinedFunction LOW_BOUNDARY_FOR_TIME_UDF = functions.udf(
      (String s) -> Utilities.lowBoundaryForTime(s, TIME_BOUNDARY_PRECISION),
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

  @FunctionalInterface
  interface Expectation {

    void expect(@Nonnull final Supplier<Dataset<Row>> result);
  }

  @Value
  static class CompositeExpectation implements Expectation {

    List<Expectation> expectations;

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
      // TODO: expect a specialized FHIRView exception
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
          })
          .collect(toList());

      // Select the data with the dynamically created column expressions.
      final Dataset<Row> selectedExpectedResult = expectedResult.select(
          asScalaBuffer(selectColumns).seq());
      final Dataset<Row> selectedActualResult = rowDataset.select(
          asScalaBuffer(selectColumns).seq());

      // Assert that the rowDataset has rows unordered as in selectedExpectedResult.
      assertThat(selectedActualResult).hasRowsAndColumnsUnordered(selectedExpectedResult);
    }

  }

  @Value
  static class ExpectCount implements ResultExpectation {

    long count;

    @Override
    public void expectResult(@Nonnull final Dataset<Row> rowDataset) {
      assertEquals(count, rowDataset.count());
    }
  }

  @Value
  static class ExpectColumns implements ResultExpectation {

    List<String> columns;

    @Override
    public void expectResult(@Nonnull final Dataset<Row> rowDataset) {
      assertArrayEquals(rowDataset.columns(), columns.toArray());
    }

  }


  /**
   * Constructor for the FhirViewTest class.
   *
   * @param includeTags the set of tags to include. Empty set means all tags are included.
   */
  protected FhirViewTest(@Nonnull final String testLocationGlob,
      @Nonnull final Set<String> includeTags) {
    this.testLocationGlob = testLocationGlob;
    this.includeTags = includeTags;
  }

  protected FhirViewTest(@Nonnull final String testLocationGlob) {
    this(testLocationGlob, Collections.emptySet());
  }


  @BeforeAll
  static void beforeAll() throws IOException {
    tempDir = Files.createTempDirectory("pathling-fhir-view-test");
    log.debug("Created temporary directory: " + tempDir);
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
          return toTestParameters(testDefinition, sourceData).stream();
        });
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
              (MapFunction<String, IBaseResource>) (json) -> jsonParser(fhirContext())
                  .parseResource(json), encoder).toDF().cache();
          result.put(ResourceType.fromCode(resourceType), resourceDataset);
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
    @Nullable
    JsonNode expectation;
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
    assumeFalse(parameters.isDisabled(), "Test is disabled");
    log.info("Running test: " + parameters.getTitle());

    parameters.getExpectation().expect(() -> {
      final FhirView view;
      try {
        // Serialize a FhirView object from the view definition in the test.
        view = gson.fromJson(parameters.getViewJson(), FhirView.class);
        ensureValid(view, "View is not valid");
      } catch (final Exception e) {
        // If parsing the view definition fails, log the JSON and rethrow the exception.
        log.info("Exception occurred while parsing test definition - " + e.getMessage());
        log.info(parameters.getViewJson());
        throw e;
      }

      // Create a new executor and build the query.
      final FhirViewExecutor executor = new FhirViewExecutor(fhirContext, spark,
          parameters.getSourceData());
      return executor.buildQuery(view);
    });
  }

  @Value
  static class TestParameters {

    @Nonnull
    String suiteName;

    @Nonnull
    String testName;

    @Nonnull
    DataSource sourceData;

    @Nonnull
    String viewJson;

    @Nonnull
    Expectation expectation;
    boolean disabled;


    @Nonnull
    public String getTitle() {
      return suiteName + " - " + testName;
    }

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

    private static final Map<ResourceType, Dataset<Row>> resourceTypeToDataset = new HashMap<>();

    public void put(@Nonnull final ResourceType resourceType, @Nonnull final Dataset<Row> dataset) {
      resourceTypeToDataset.put(resourceType, dataset);
    }

    @Nonnull
    @Override
    public Dataset<Row> read(@Nullable final ResourceType resourceType) {
      return resourceTypeToDataset.get(resourceType);
    }

    @Nonnull
    @Override
    public Dataset<Row> read(@Nullable final String resourceCode) {
      return resourceTypeToDataset.get(ResourceType.fromCode(resourceCode));
    }

    @Nonnull
    @Override
    public Set<ResourceType> getResourceTypes() {
      return resourceTypeToDataset.keySet();
    }
  }
}
