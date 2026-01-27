package au.csiro.pathling.examples;

import static au.csiro.pathling.views.FhirView.column;
import static au.csiro.pathling.views.FhirView.columns;
import static au.csiro.pathling.views.FhirView.forEach;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.views.FhirView;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PathlingJavaApp {

  public static void main(String[] args) {
    PathlingContext pc = PathlingContext.create();
    final QueryableDataSource data = pc.read().ndjson("/tmp/ndjson");

    final FhirView view =
        FhirView.ofResource("Observation")
            .select(
                columns(column("patient_id", "getResourceKey()")),
                forEach(
                    "code.coding",
                    column("code_system", "system"),
                    column("code_code", "code"),
                    column("code_display", "display")))
            .build();

    final Dataset<Row> result = data.view(view).execute();

    result.show(false);
  }
}
