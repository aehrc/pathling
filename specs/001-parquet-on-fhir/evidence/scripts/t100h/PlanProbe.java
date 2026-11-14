import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.evaluation.CrossResourceStrategy;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.test.layout.LayoutDatasets;
import au.csiro.pathling.test.layout.TestLayout;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Patient;

/** Throwaway probe for T100h: the optimised plans behind the #2594 entry counts. */
public class PlanProbe {

  public static void main(final String[] args) {
    final SparkSession spark =
        SparkSession.builder().master("local[1]").config("spark.ui.enabled", false).getOrCreate();
    final FhirEncoders encoders =
        FhirEncoders.forR4().withExtensionsEnabled(true).withAllOpenTypes().getOrCreate();
    final Patient p = new Patient();
    p.setId("p1");
    final Dataset<Row> local =
        LayoutDatasets.fromResources(spark, encoders, TestLayout.PREVIOUS, "Patient", List.of(p));
    local.write().mode("overwrite").parquet(args[0] + "/plan-patient");
    final Dataset<Row> parquet = spark.read().parquet(args[0] + "/plan-patient");
    for (final Dataset<Row> df : List.of(local, parquet)) {
      final DatasetEvaluator ev =
          DatasetEvaluatorBuilder.create(ResourceType.PATIENT, encoders.getContext())
              .withDataset(df)
              .withCrossResourceStrategy(CrossResourceStrategy.EMPTY)
              .build();
      final Dataset<Row> result =
          ev.evaluate(new Parser().parse("'a'.trace('t') = 1")).toCanonical().toIdValueDataset();
      System.out.println("---- analysed");
      System.out.println(result.queryExecution().analyzed().treeString());
      System.out.println("---- optimised");
      System.out.println(result.queryExecution().optimizedPlan().treeString());
    }
    spark.stop();
  }
}
