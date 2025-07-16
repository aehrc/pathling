package au.csiro.pathling.examples;

import static au.csiro.pathling.views.FhirView.column;
import static au.csiro.pathling.views.FhirView.columns;
import static au.csiro.pathling.views.FhirView.forEach;
import static au.csiro.pathling.views.FhirView.forEachOrNull;
import static au.csiro.pathling.views.FhirView.select;
import static au.csiro.pathling.views.FhirView.unionAll;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.NdjsonSource;
import au.csiro.pathling.views.FhirView;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.StringType;

class FhirViewExample {

  public static void main(String[] args) {
    PathlingContext pc = PathlingContext.create();
    NdjsonSource data = pc.read()
        .ndjson("/Users/gri306/Library/CloudStorage/OneDrive-CSIRO/Data/synthea/paper-sm/fhir");

    FhirView view = FhirView.ofResource(ResourceType.OBSERVATION)
        .constant("vital_signs", new StringType("vital-signs"))
        .select(
            columns(
                column("patient_id", "getResourceKey()")
            ),
            forEach("code.coding",
                column("code_system", "system"),
                column("code_code", "code"),
                column("code_display", "display")
            ),
            forEachOrNull("interpretation.coding",
                column("interpretation_system", "system"),
                column("interpretation_code", "code"),
                column("interpretation_display", "display")
            ),
            unionAll(
                select(
                    column("value", "value.ofType(dateTime)")
                ),
                select(
                    column("value", "value.ofType(Period).start")
                )
            )
        )
        .where(
            "category.coding.exists(system = 'http://terminology.hl7.org/CodeSystem/observation-category' and code = %vital_signs)",
            "status = 'final'"
        ).build();

    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    System.out.println(gson.toJson(view));

    Dataset<Row> result = data.view(view).execute();

    result.show();
  }
}
