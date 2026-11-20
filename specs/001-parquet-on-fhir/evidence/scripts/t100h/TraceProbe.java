import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.ListTraceCollector;
import au.csiro.pathling.fhirpath.TraceCollectorProxy;
import au.csiro.pathling.fhirpath.evaluation.CrossResourceStrategy;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.evaluation.SingleInstanceEvaluationResult;
import au.csiro.pathling.fhirpath.evaluation.SingleInstanceEvaluator;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.test.layout.LayoutDatasets;
import au.csiro.pathling.test.layout.TestLayout;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.HumanName;
import org.hl7.fhir.r4.model.Patient;

/** Throwaway probe for T100h: why the trace tests depend on the layout. */
public class TraceProbe {

  public static void main(final String[] args) throws Exception {
    final String out = args[0];
    final SparkSession spark =
        SparkSession.builder()
            .master("local[1]")
            .config("spark.ui.enabled", false)
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.driver.host", "localhost")
            .getOrCreate();
    final FhirEncoders encoders =
        FhirEncoders.forR4().withExtensionsEnabled(true).withAllOpenTypes().getOrCreate();
    final Patient p = new Patient();
    p.setId("p1");
    p.setActive(true);
    p.addName(new HumanName().setFamily("Smith").addGiven("Jane").setUse(HumanName.NameUse.OFFICIAL));
    p.addName(new HumanName().setFamily("Doe").addGiven("John"));
    p.addName(new HumanName().setFamily("Roe").addGiven("Rick"));
    final List<IBaseResource> resources = List.of(p);

    final Dataset<Row> previousLocal =
        LayoutDatasets.fromResources(spark, encoders, TestLayout.PREVIOUS, "Patient", resources);
    previousLocal.write().mode("overwrite").parquet(out + "/previous-patient");
    final Dataset<Row> previousParquet = spark.read().parquet(out + "/previous-patient");
    final Dataset<Row> pof =
        LayoutDatasets.fromResources(spark, encoders, TestLayout.POF, "Patient", resources);

    System.out.println("== plan roots");
    System.out.println("previous-local: " + previousLocal.queryExecution().logical().getClass().getSimpleName());
    System.out.println("pof: " + pof.queryExecution().optimizedPlan().treeString());

    System.out.println("== Java serialisation of ListTraceCollector");
    try (ObjectOutputStream o = new ObjectOutputStream(new ByteArrayOutputStream())) {
      o.writeObject(new ListTraceCollector());
      System.out.println("serialised");
    } catch (final Exception e) {
      System.out.println("failed: " + e);
    }

    final String[] expressions = {
      "'a'.trace('t')", "'a'.trace('t') = 1", "'a' = 1.trace('t')", "'a'.trace('t') = 'a'",
      "Patient.name.family.first().trace('t') = 'Smith'"
    };
    System.out.println("== SingleInstanceEvaluator trace counts (label t)");
    for (final String[] pair :
        new String[][] {{"previous-local"}, {"previous-parquet"}, {"pof"}}) {
      final Dataset<Row> df =
          switch (pair[0]) {
            case "previous-local" -> previousLocal;
            case "previous-parquet" -> previousParquet;
            default -> pof;
          };
      for (final String e : expressions) {
        final SingleInstanceEvaluationResult r =
            SingleInstanceEvaluator.evaluate(df, "Patient", encoders.getContext(), e, null, null);
        final long n =
            r.getTraces().stream()
                .filter(t -> "t".equals(t.getLabel()))
                .mapToLong(t -> t.getValues().size())
                .sum();
        System.out.println(pair[0] + " | " + e + " | entries=" + n);
      }
    }

    System.out.println("== DatasetEvaluator with a raw and a proxied ListTraceCollector");
    final Parser parser = new Parser();
    for (final String name : new String[] {"previous-local", "previous-parquet", "pof"}) {
      final Dataset<Row> df =
          switch (name) {
            case "previous-local" -> previousLocal;
            case "previous-parquet" -> previousParquet;
            default -> pof;
          };
      for (final boolean proxied : new boolean[] {false, true}) {
        final ListTraceCollector collector = new ListTraceCollector();
        final TraceCollectorProxy proxy = TraceCollectorProxy.create(collector);
        try {
          final DatasetEvaluator ev =
              DatasetEvaluatorBuilder.create(ResourceType.PATIENT, encoders.getContext())
                  .withDataset(df)
                  .withCrossResourceStrategy(CrossResourceStrategy.EMPTY)
                  .withTraceCollector(proxied ? proxy : collector)
                  .build();
          ev.evaluate(parser.parse("Patient.name.trace('names')"))
              .toCanonical()
              .toIdValueDataset()
              .collectAsList();
          System.out.println(
              name + " | proxied=" + proxied + " | ok, entries=" + collector.getEntries().size()
                  + " types=" + collector.getEntries().stream().map(x -> x.fhirType()).distinct().toList());
        } catch (final Throwable t) {
          Throwable root = t;
          while (root.getCause() != null) {
            root = root.getCause();
          }
          System.out.println(name + " | proxied=" + proxied + " | FAILED " + t + " / root " + root);
        } finally {
          proxy.close();
        }
      }
    }
    spark.stop();
  }
}
