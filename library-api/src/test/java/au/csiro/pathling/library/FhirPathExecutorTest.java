package au.csiro.pathling.library;

import au.csiro.pathling.fhirpath.evaluation.DefaultEvaluationContext;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.fhirpath.function.registry.StaticOperatorRegistry;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.io.source.NdjsonSource;
import au.csiro.pathling.test.TestResources;
import ca.uhn.fhir.context.FhirVersionEnum;
import java.net.URL;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FhirPathExecutorTest {

  @BeforeEach
  void setUp() {
  }

  @Test
  void test() {
    final SparkSession spark = SparkSession.builder()
        .master("local[*]")
        .getOrCreate();
    final StaticFunctionRegistry functionRegistry = new StaticFunctionRegistry();
    final StaticOperatorRegistry operatorRegistry = new StaticOperatorRegistry();
    final EvaluationContext context = new DefaultEvaluationContext(spark, functionRegistry,
        operatorRegistry);
    final URL testData = TestResources.getResourceAsUrl("bulk/fhir");
    final DataSource dataSource = new NdjsonSource(testData.toString(),
        FhirVersionEnum.R4.getFhirVersionString());
    final FhirPathExecutor executor = new FhirPathExecutor(dataSource, context);
    final Dataset<Row> result = executor.execute("Patient",
        "name.where(use = 'official').first().family", "result");
    result.show(false);
  }

  @AfterEach
  void tearDown() {
  }

}
