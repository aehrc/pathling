package au.csiro.pathling.fhirpath.yaml;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.definition.DefinitionContext;
import au.csiro.pathling.fhirpath.definition.ResourceDefinition;
import au.csiro.pathling.fhirpath.definition.def.DefDefinitionContext;
import au.csiro.pathling.fhirpath.definition.def.DefResourceDefinition;
import au.csiro.pathling.fhirpath.definition.def.DefResourceTag;
import au.csiro.pathling.fhirpath.execution.DefResourceResolver;
import au.csiro.pathling.fhirpath.execution.FhirpathEvaluator;
import au.csiro.pathling.fhirpath.execution.StdFhirpathEvaluator;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.test.SpringBootUnitTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.yaml.snakeyaml.Yaml;


@SpringBootUnitTest
@Tag("WorkTest")
public class YamlTest {


  @Autowired
  SparkSession spark;

  private static final Yaml YAML_PARSER = new Yaml();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();


  @Nonnull
  static String yamlToJsonResource(@Nonnull final String yamlData) throws Exception {
    final Map<String, Object> data = YAML_PARSER.load(yamlData);
    // Serialize the data map into a JSON string
    return OBJECT_MAPPER.writeValueAsString(data);
  }


  @Test
  void testSimpleYaml() throws Exception {

    String x =
        """
            resourceType: MathTestData
            n1: 2
            n2: 5
            a3:
              - 1
              - 2
            i: null
            _i:
              id: nullId
            n4: []
            s5: "one"
            s6: "two"
            b1: true
            f1: 1.0
            """;

    final Map<String, Object> yamlData = YAML_PARSER.load(x);
    DefResourceDefinition def = (DefResourceDefinition) YamlSupport.yamlToDefinition("Test",
        yamlData);
    System.out.println("Yaml definition:");
    System.out.println(def);

    final StructType struct = YamlSupport.defnitiontoStruct(def);
    System.out.println("Struct definition:");
    struct.printTreeString();

    System.out.println(yamlToJsonResource(x));

    final Dataset<Row> inputDS = spark.read().schema(struct)
        .json(spark.createDataset(List.of(yamlToJsonResource(x)),
            Encoders.STRING()));

    inputDS.printSchema();
    inputDS.show();

    final ResourceDefinition resouceDefinition = def;
    final DefinitionContext definitionContext = DefDefinitionContext.of(resouceDefinition);

    final FhirpathEvaluator evaluator = new StdFhirpathEvaluator(
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

    System.out.println("Evaluating expression: a3.first() * n2");
    for (int i = 0; i < 10; i++) {
      final Collection result = evaluator.evaluate(
          parser.parse("a3.first() * n2 + " + i));
      ds.select(result.getColumnValue().alias("result")).first();
    }
    System.out.println("End expression: a3.first() * n2");

    // final Column[] columns = IntStream.range(0, 1000).mapToObj(i -> "a3.first() * n2 + " + i)
    //     .map(parser::parse)
    //     .map(evaluator::evaluate)
    //     .map(Collection::getColumnValue)
    //     .toArray(Column[]::new);
    // ds.select(columns).first();
    // System.out.println("End batched: a3.first() * n2");

  }

}
