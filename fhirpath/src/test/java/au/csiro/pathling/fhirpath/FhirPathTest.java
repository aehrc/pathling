package au.csiro.pathling.fhirpath;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.fhirpath.collection.rendering.RootRendering;
import au.csiro.pathling.fhirpath.evaluation.DefaultEvaluationContext;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.fhirpath.function.registry.StaticOperatorRegistry;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.schema.FhirJsonReader;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.yaml.snakeyaml.Yaml;
import scala.collection.JavaConverters;

@Slf4j
public class FhirPathTest {

  static final String TEST_LOCATION = "classpath:cases/";
  static final String RESOURCE_LOCATION = "classpath:resources/";
  static final String TEST_GLOB = "*.yaml";
  static final Yaml YAML = new Yaml();
  static final ObjectMapper MAPPER = new ObjectMapper();
  static final Map<String, String> MODEL_TO_FHIR_VERSION = Map.of(
      "dstu2", "1.0",
      "stu3", "3.0",
      "r4", "4.0",
      "r5", "5.0"
  );

  @BeforeAll
  static void beforeAll() {
    SparkSession.builder().master("local[*]").appName("FhirPathTest").getOrCreate();
  }

  static @NotNull Stream<TestParameters> parameters() throws IOException {
    final Collection<TestParameters> result = new ArrayList<>();
    final ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
    final Resource[] resources = resolver.getResources(TEST_LOCATION + TEST_GLOB);
    for (final Resource resource : resources) {
      final Path path = resource.getFile().toPath();
      try (final InputStream is = Files.newInputStream(path)) {
        final Map<String, Object> testCase = YAML.load(is);

        final Object subject = testCase.get("subject");
        final String resourceType;
        if (!(subject instanceof Map)) {
          resourceType = null;
        } else {
          //noinspection unchecked
          resourceType = (String) ((Map<String, Object>) subject).get("resourceType");
        }

        List<String> jsonStrings = null;
        if (subject != null) {
          final String jsonString = MAPPER.writer().writeValueAsString(subject);
          jsonStrings = List.of(jsonString);
        }

        @SuppressWarnings("unchecked")
        final Iterable<Map<String, Object>> maybeGroups = (List<Map<String, Object>>) testCase.get(
            "tests");

        if (maybeGroups.iterator().hasNext()) {
          final Map<String, Object> firstGroup = maybeGroups.iterator().next();
          final Optional<String> firstKey = firstGroup.keySet().stream().findFirst();
          if (firstKey.isPresent() && firstKey.get().startsWith("group:")) {
            for (final Map<String, Object> group : maybeGroups) {
              // Check if the group is disabled.
              if (group.containsKey("disable") && (Boolean) group.get("disable")) {
                continue;
              }

              for (final String groupName : group.keySet()) {
                @SuppressWarnings("unchecked")
                final Iterable<Map<String, Object>> tests = (List<Map<String, Object>>) group.get(
                    groupName);
                result.addAll(extractTests(tests, jsonStrings, resourceType, groupName));
              }
            }
          } else {
            result.addAll(extractTests(maybeGroups, jsonStrings, resourceType, null));
          }
        }
      }
    }
    return result.stream();
  }

  static @NotNull List<TestParameters> extractTests(
      @NotNull final Iterable<Map<String, Object>> tests,
      @Nullable final List<String> jsonStrings, @Nullable final String resourceType,
      @Nullable final String groupName) {
    final List<TestParameters> result = new ArrayList<>();
    for (final Map<String, Object> test : tests) {
      // Check if the test is disabled.
      if (test.containsKey("disable") && (Boolean) test.get("disable")) {
        continue;
      }

      final Object expressionObj = test.get("expression");
      if (expressionObj == null) {
        continue;
      }
      if (expressionObj instanceof List<?>) {
        final Collection<Map<String, Object>> expressionTests = new ArrayList<>();
        //noinspection unchecked
        for (final Object expression : (List<String>) expressionObj) {
          final Map<String, Object> expressionTest = new HashMap<>(test);
          expressionTest.put("expression", expression);
          expressionTests.add(expressionTest);
        }
        result.addAll(extractTests(expressionTests, jsonStrings, resourceType, groupName));
        return result;
      }
      final String expression = (String) test.get("expression");
      final String desc = (String) test.get("desc");
      final String description = groupName != null && desc != null
                                 ? groupName + " - " + desc
                                 : Objects.requireNonNullElse(desc, expression);

      @SuppressWarnings("unchecked") final List<Object> expectedResult = (List<Object>) test.get(
          "result");
      final boolean error = Optional.ofNullable(test.get("error"))
          .map(e -> (Boolean) e).orElse(false);
      final String model = Optional.ofNullable((String) test.get("model"))
          .map(MODEL_TO_FHIR_VERSION::get).orElse(null);

      List<String> data = jsonStrings;
      final String inputFile = (String) test.get("inputfile");
      if (inputFile != null) {
        final Path path = getResourcePath(inputFile);
        try (final InputStream is = Files.newInputStream(path)) {
          final StringBuilder resultStringBuilder = new StringBuilder();
          try (final BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
            String line;
            while ((line = br.readLine()) != null) {
              resultStringBuilder.append(line).append("\n"); // Preserves line breaks
            }
          }
          data = List.of(resultStringBuilder.toString());
        } catch (final IOException e) {
          throw new RuntimeException(e);
        }
      }

      result.add(
          new TestParameters(expression, error, data, expectedResult, description, model,
              resourceType));
    }
    return result;
  }

  static @NotNull Path getResourcePath(final @NotNull String inputFile) {
    final String inputPath = RESOURCE_LOCATION + inputFile;
    final ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
    final Resource[] resources;
    try {
      resources = resolver.getResources(inputPath);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    if (resources.length != 1) {
      throw new RuntimeException("Expected exactly one resource at " + inputPath);
    }
    final Path path;
    try {
      path = resources[0].getFile().toPath();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    return path;
  }

  static @NotNull List<Row> evaluateAndExecute(final @NotNull FhirPath parsed,
      final @NotNull au.csiro.pathling.fhirpath.collection.Collection inputCollection,
      final @NotNull EvaluationContext context, final @NotNull Dataset<Row> input) {
    final au.csiro.pathling.fhirpath.collection.Collection output = parsed.evaluate(inputCollection,
        context);
    final Optional<Column> column = output.getRendering().getColumn();
    if (column.isEmpty()) {
      throw new RuntimeException("Expected a column rendering in the evaluation result");
    }
    final Dataset<Row> result = input.select(column.get().alias("result"));
    return result.collectAsList();
  }

  @ParameterizedTest
  @MethodSource("parameters")
  void test(@NotNull final TestParameters parameters) {
    log.info("Testing {}", parameters.description);
    log.info("Expression: {}", parameters.expression);
    log.info("Expected result: {}", parameters.expectedResults);
    log.info("Error expected: {}", parameters.error);
    log.info("Input data: {}", parameters.data);
    log.info("FHIR model version: {}", parameters.model);
    log.info("Resource type: {}", parameters.resourceType);

    final SparkSession spark = SparkSession.active();

    final FhirJsonReader reader =
        parameters.model != null && parameters.resourceType != null
        ? new FhirJsonReader(parameters.resourceType, parameters.model, Map.of())
        : new FhirJsonReader();

    final Dataset<Row> input;
    if (parameters.data != null) {
      input = reader.read(parameters.data);
    } else {
      // If no data is provided, create an dataset with a single row.
      final StructType schema = new StructType(new StructField[]{
          DataTypes.createStructField("id", DataTypes.StringType, false)
      });
      final Row row = RowFactory.create("1");
      final List<Row> rowData = List.of(row);
      input = spark.createDataFrame(rowData, schema);
    }

    final StaticFunctionRegistry functionRegistry = new StaticFunctionRegistry();
    final StaticOperatorRegistry operatorRegistry = new StaticOperatorRegistry();
    final EvaluationContext context = new DefaultEvaluationContext(spark,
        functionRegistry, operatorRegistry);

    final Parser parser = new Parser();
    final FhirPath parsed = parser.parse(parameters.expression);

    final au.csiro.pathling.fhirpath.collection.Collection inputCollection;
    if (parameters.data == null) {
      inputCollection = au.csiro.pathling.fhirpath.collection.Collection.empty();
    } else {
      final Optional<FhirPathType> type = Optional.ofNullable(parameters.resourceType)
          .map(resourceType -> new FhirPathType("FHIR", resourceType));
      final Map<String, Column> columnMap = new HashMap<>();
      for (final String field : input.columns()) {
        columnMap.put(field, functions.col(field));
      }
      inputCollection = new au.csiro.pathling.fhirpath.collection.Collection(
          new RootRendering(columnMap), type);
    }

    if (parameters.error) {
      assertThrows(Exception.class,
          () -> evaluateAndExecute(parsed, inputCollection, context, input));
      return;
    }
    final List<Row> rows = evaluateAndExecute(parsed, inputCollection, context, input);

    assertEquals(1, rows.size());
    final Row row = rows.get(0);
    final Object rawValue = row.get(row.fieldIndex("result"));
    final List<Object> value;
    if (rawValue instanceof scala.collection.mutable.WrappedArray) {
      //noinspection unchecked
      value = JavaConverters.seqAsJavaListConverter(
          (scala.collection.mutable.WrappedArray<Object>) rawValue).asJava();
    } else if (rawValue instanceof List) {
      //noinspection unchecked
      value = (List<Object>) rawValue;
    } else {
      value = Collections.singletonList(rawValue);
    }
    @SuppressWarnings("unchecked") final List<Object> resultList = value;

    assertEquals(parameters.expectedResults, resultList);
  }

  record TestParameters(@NotNull String expression, boolean error, @Nullable List<String> data,
                        @Nullable List<Object> expectedResults, @Nullable String description,
                        @Nullable String model, @Nullable String resourceType) {

    @Override
    public String toString() {
      return Optional.ofNullable(description).orElse(expression);
    }
  }

}
