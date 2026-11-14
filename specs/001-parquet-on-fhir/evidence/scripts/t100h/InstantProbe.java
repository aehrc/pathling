import static au.csiro.pathling.views.FhirView.columns;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.evaluation.CrossResourceStrategy;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import au.csiro.pathling.test.layout.TestLayout;
import au.csiro.pathling.views.Column;
import au.csiro.pathling.views.ColumnTag;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewExecutor;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.Observation;

/** Throwaway probe for T100h: how an instant behaves on each layout. */
public class InstantProbe {

  public static void main(final String[] args) {
    final SparkSession spark =
        SparkSession.builder().master("local[1]").config("spark.ui.enabled", false).getOrCreate();
    final FhirEncoders encoders =
        FhirEncoders.forR4().withExtensionsEnabled(true).withAllOpenTypes().getOrCreate();
    au.csiro.pathling.sql.PathlingUdfConfigurer.registerUdfs(spark);
    final Observation o = new Observation();
    o.setId("o1");
    o.setIssuedElement(new InstantType("2023-01-01T12:00:00+10:00"));
    final List<IBaseResource> resources = List.of(o);
    final String[] expressions = {
      "issued",
      "issued.toString()",
      "issued = @2023-01-01T02:00:00Z",
      "issued = @2023-01-01T12:00:00+10:00",
      "issued > @2023-01-01T01:30:00Z",
      "issued < @2023-01-01T02:30:00Z",
      "issued ~ @2023-01-01T02:00:00Z",
    };
    for (final TestLayout layout : List.of(TestLayout.PREVIOUS, TestLayout.POF)) {
      final ObjectDataSource ds = new ObjectDataSource(spark, encoders, resources, layout);
      System.out.println("== " + layout + " stored issued type: "
          + ds.read("Observation").schema().apply("issued").dataType());
      final DatasetEvaluator ev =
          DatasetEvaluatorBuilder.create(ResourceType.OBSERVATION, encoders.getContext())
              .withDataset(ds.read("Observation"))
              .withCrossResourceStrategy(CrossResourceStrategy.EMPTY)
              .build();
      for (final String e : expressions) {
        try {
          final Dataset<Row> r =
              ev.evaluate(new Parser().parse(e)).toCanonical().toIdValueDataset();
          System.out.println(layout + " | " + e + " | " + r.schema().apply(1).dataType() + " | "
              + r.collectAsList().get(0).get(1));
        } catch (final Exception ex) {
          System.out.println(layout + " | " + e + " | ERROR " + ex);
        }
      }
      final FhirViewExecutor exec = new FhirViewExecutor(encoders.getContext(), ds);
      final Object[][] cols = {
        {"untyped", null, null},
        {"declared-instant", "instant", null},
        {"ansi-ts", null, "TIMESTAMP WITH TIME ZONE"},
        {"ansi-ntz", null, "TIMESTAMP WITHOUT TIME ZONE"},
      };
      for (final Object[] c : cols) {
        Column.ColumnBuilder b = Column.builder().name("value").path("issued");
        if (c[1] != null) {
          b = b.type((String) c[1]);
        }
        if (c[2] != null) {
          b = b.tag(List.of(ColumnTag.of("ansi/type", (String) c[2])));
        }
        final FhirView view = FhirView.ofResource("Observation").select(columns(b.build())).build();
        try {
          final Dataset<Row> r = exec.buildQuery(view);
          System.out.println(layout + " | view " + c[0] + " | " + r.schema().apply(0).dataType()
              + " | " + r.first().get(0));
        } catch (final Exception ex) {
          System.out.println(layout + " | view " + c[0] + " | ERROR " + ex);
        }
      }
    }
    spark.stop();
  }
}
