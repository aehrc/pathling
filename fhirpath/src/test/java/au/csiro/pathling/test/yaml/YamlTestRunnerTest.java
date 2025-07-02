package au.csiro.pathling.test.yaml;

import static au.csiro.pathling.test.TestResources.getResourceAsString;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.definition.ChildDefinition;
import au.csiro.pathling.fhirpath.definition.DefinitionContext;
import au.csiro.pathling.fhirpath.definition.def.DefDefinitionContext;
import au.csiro.pathling.fhirpath.definition.def.DefResourceDefinition;
import au.csiro.pathling.fhirpath.definition.def.DefResourceTag;
import au.csiro.pathling.fhirpath.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.fhirpath.definition.fhir.FhirResourceTag;
import au.csiro.pathling.fhirpath.execution.DefResourceResolver;
import au.csiro.pathling.fhirpath.execution.FhirpathEvaluator;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.test.SpringBootUnitTest;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.yaml.snakeyaml.Yaml;


@SpringBootUnitTest
@Tag("WorkTest")
class YamlTestRunnerTest {


  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  @Autowired
  FhirEncoders fhirEncoders;

  private static final Yaml YAML_PARSER = new Yaml();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();


  @SuppressWarnings("SameParameterValue")
  @Nonnull
  private static String yamlToJsonResource(@Nonnull final String yamlData) throws Exception {
    final Map<Object, Object> data = YAML_PARSER.load(yamlData);
    // Serialize the data map into a JSON string
    return OBJECT_MAPPER.writeValueAsString(data);
  }


  @Test
  void testSimpleYaml() throws Exception {

    final String subjectString =
        """
              el:
                a: 2
              not_el:
                a: 4
              coll2:
                - a: 3
                - a: 4
                - a: 5
              coll:
                - a: 1
                - a: 2
                - a: 3
              emptycoll: []
              il: 2
              icoll:
                - 1
                - 2
                - 3
            """;

    final Map<Object, Object> subjectYamlModel = YAML_PARSER.load(subjectString);
    final DefResourceDefinition subjectDefinition = (DefResourceDefinition) YamlSupport.yamlToDefinition(
        "Test",
        subjectYamlModel);

    System.out.println("Yaml definition:");
    System.out.println(subjectDefinition);

    final StructType subjectSchema = YamlSupport.defnitiontoStruct(subjectDefinition);
    System.out.println("Struct definition:");
    subjectSchema.printTreeString();

    System.out.println(yamlToJsonResource(subjectString));

    final Dataset<Row> inputDS = spark.read().schema(subjectSchema)
        .json(spark.createDataset(List.of(yamlToJsonResource(subjectString)),
            Encoders.STRING()));

    inputDS.printSchema();
    inputDS.show();

    final DefinitionContext definitionContext = DefDefinitionContext.of(subjectDefinition);
    final FhirpathEvaluator evaluator = new FhirpathEvaluator(
        DefResourceResolver.of(
            DefResourceTag.of("Test"),
            definitionContext,
            inputDS
        ),
        StaticFunctionRegistry.getInstance(),
        Map.of()
    );

    final Dataset<Row> ds = evaluator.createInitialDataset().cache();
    final Parser parser = new Parser();

    final String testCaseStr = """
        desc: '6.4.2 in'
        expression: 'il combine icoll'
        result: [2,1,2,3]
        """;

    final Map<String, Object> testCase = YAML_PARSER.load(testCaseStr);

    final String expression = (String) testCase.get("expression");
    final String desc = (String) testCase.get("desc");

    // lets try something different here
    // pethaps I couild flatten the result representation here as well

    final Object result = testCase.get("result");
    final Object resultRepresentation = result instanceof final List<?> list && list.size() == 1
                                        ? list.get(0)
                                        : result;

    final ChildDefinition resultDefinition = YamlSupport.elementFromYaml(
        "result",
        resultRepresentation);
    // now lets create child schema
    final StructType resultSchema = YamlSupport.childrendToStruct(List.of(resultDefinition));
    System.out.println("Result schema:");
    resultSchema.printTreeString();
    // now we will need to create a column out of it based on the json mapping.
    final String resultJson = OBJECT_MAPPER.writeValueAsString(
        Map.of("result", resultRepresentation));
    System.out.println("Result json: " + resultJson);

    final ColumnRepresentation expectedRepresentation = new DefaultRepresentation(
        functions.from_json(functions.lit(resultJson),
            resultSchema).getField("result"));

    System.out.println("Evaluating: `" + desc + "` with: `" + expression + "`");
    final Collection evalResult = evaluator.evaluate(
        parser.parse(expression));
    final Row resultRow = ds.select(
        evalResult.getColumn().asCanonical().getValue().alias("actual"),
        expectedRepresentation.getValue().alias("expected")
    ).first();

    resultRow.schema().printTreeString();
    System.out.println("Result row: " + resultRow);

    final Object actual = resultRow.isNullAt(0)
                          ? null
                          : resultRow.get(0);

    final Object expected = resultRow.isNullAt(1)
                            ? null
                            : resultRow.get(1);

    assertEquals(expected, actual, "Expected: " + expected + " but got: " + actual);
  }

  @Test
  void testLoad() {
    final String testSpec = getResourceAsString("fhirpath-js/cases/5.1_existence.yaml");
    final YamlTestDefinition spec = YamlTestDefinition.fromYaml(testSpec);
    System.out.println(spec);
  }

  @Test
  void testLoadAndRun() {
    final String testConfigYaml = getResourceAsString("fhirpath-js/config.yaml");
    final TestConfig testConfig = YAML_PARSER.loadAs(testConfigYaml, TestConfig.class);
    System.out.println(testConfig);
    testConfig.toPredicates(Set.of()).forEach(System.out::println);
  }


  @Test
  void testJsonModel() {
    final String testPatient = getResourceAsString("fhirpath-js/resources/patient-example-2.json");
    System.out.println(testPatient);

    final IParser jsonParser = fhirContext.newJsonParser();
    final IBaseResource resource = jsonParser.parseResource(
        testPatient);
    System.out.println(resource);
    System.out.println(resource.fhirType());
    final Dataset<Row> inputDS = spark.createDataset(List.of(resource),
        fhirEncoders.of(resource.fhirType())).toDF();

    final DefResourceResolver resolver = DefResourceResolver.of(
        FhirResourceTag.of(ResourceType.fromCode(resource.fhirType())),
        FhirDefinitionContext.of(fhirContext),
        inputDS
    );

    final FhirpathEvaluator evaluator = new FhirpathEvaluator(
        resolver,
        StaticFunctionRegistry.getInstance(),
        Map.of()
    );

    final Dataset<Row> ds = evaluator.createInitialDataset().cache();
    final Parser parser = new Parser();

    final Collection result = evaluator.evaluate(parser.parse("Patient.name"));
    ds.select(result.getColumn().asCanonical().getValue().alias("actual")).show();
  }
}
