package au.csiro.pathling.views;

import static au.csiro.pathling.UnitTestDependencies.fhirContext;
import static au.csiro.pathling.UnitTestDependencies.jsonParser;
import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.validation.ValidationUtils.ensureValid;
import static java.util.Objects.nonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.SpringBootUnitTest;
import ca.uhn.fhir.context.FhirContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
@Slf4j
abstract class AbstractFhirViewTestBase {

  static Path tempDir;

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  @Autowired
  FhirEncoders fhirEncoders;

  @Autowired
  Gson gson;

  @MockBean
  TerminologyServiceFactory terminologyServiceFactory;


  private final String testLocationGlob;


  @FunctionalInterface
  interface Expectation {

    void expect(@Nonnull final Supplier<Dataset<Row>> result);
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

    Path expectedJson;
    List<String> expectedColumns;

    @Override
    public void expectResult(@Nonnull final Dataset<Row> rowDataset) {
      final Dataset<Row> expectedResult = spark.read().json(expectedJson.toString())
          .selectExpr(expectedColumns.toArray(new String[0]));
      assertThat(rowDataset).hasRowsUnordered(expectedResult);
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


  protected AbstractFhirViewTestBase(final String testLocationGlob) {
    this.testLocationGlob = testLocationGlob;
  }

  @BeforeAll
  static void beforeAll() throws IOException {
    System.out.println("Creating temp directory");
    tempDir = Files.createTempDirectory("pathling-fhir-view-test");
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
    try {
      // Create a parent directory based upon the test name.
      final JsonNode resources = testDefinition.get("resources");
      final Path directory = getTempDir(testDefinition);
      final TestDataSource result = new TestDataSource();

      for (final Iterator<JsonNode> it = resources.elements(); it.hasNext(); ) {
        final JsonNode resource = it.next();

        // Append each resource to a file named after its type.
        final String resourceType = resource.get("resourceType").asText();
        final Path ndjsonPath = directory.resolve(resourceType + ".ndjson");
        Files.write(ndjsonPath,
            (resource + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE,
            StandardOpenOption.APPEND);

        // Read the NDJSON file into a Spark dataset and add it to the data source.
        final Dataset<String> jsonStrings = spark.read().text(ndjsonPath.toString())
            .as(Encoders.STRING());
        final ExpressionEncoder<IBaseResource> encoder = fhirEncoders.of(resourceType);
        final Dataset<Row> dataset = jsonStrings.map(
            (MapFunction<String, IBaseResource>) (json) -> jsonParser(fhirContext())
                .parseResource(json), encoder).toDF().cache();
        result.put(ResourceType.fromCode(resourceType), dataset);
      }

      return result;

    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  List<TestParameters> toTestParameters(@Nonnull final JsonNode testDefinition,
      @Nonnull final DataSource sourceData) {
    try {
      final JsonNode views = testDefinition.get("tests");
      final List<TestParameters> result = new ArrayList<>();

      int testNumber = 0;
      for (final Iterator<JsonNode> it = views.elements(); it.hasNext(); ) {
        final JsonNode view = it.next();

        final FhirView fhirView;
        try {
          // Serialize a FhirView object from the view definition in the test.
          fhirView = gson.fromJson(view.get("view").toString(), FhirView.class);
          ensureValid(fhirView, "View is not valid");
        } catch (final Exception e) {
          log.info("Exception occurred while parsing test definition:");
          log.info(view.toPrettyString());
          throw e;
        }

        // Write the expected JSON to a file, named after the view.
        final Path directory = getTempDir(testDefinition);
        final String expectedFileName =
            String.format("%02d_%s.json", testNumber,
                view.get("title").asText().replaceAll("\\W+", "_"));
        final Path expectedPath = directory.resolve(expectedFileName);

        final String testName =
            testDefinition.get("title").asText() + " - " + view.get("title").asText();
        final boolean disabled = Optional.ofNullable(view.get("disabled"))
            .map(JsonNode::asBoolean).orElse(false);
        result.add(
            new TestParameters(testName, sourceData, fhirView, getExpectation(view, expectedPath),
                disabled));
        testNumber++;
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
    } else if (nonNull(expectation = testDefinition.get("expect"))) {
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
        Files.write(expectedPath, (row + "\n").getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.APPEND);
      }
      return new Expect(expectedPath, expectedColumns != null
                                      ? expectedColumns
                                      : Collections.emptyList());
    } else {
      log.info("No expectation found for test:");
      log.info(testDefinition.toPrettyString());
      throw new RuntimeException("No expectation found");
    }
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

    parameters.getExpectation().expect(() -> {
      final FhirView view = parameters.getView();
      final FhirViewExecutor executor = new FhirViewExecutor(fhirContext, spark,
          parameters.getSourceData(), Optional.ofNullable(terminologyServiceFactory));
      return executor.buildQuery(view);
    });
  }

  @Value
  static class TestParameters {

    String title;
    DataSource sourceData;
    FhirView view;
    Expectation expectation;
    boolean disabled;

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
