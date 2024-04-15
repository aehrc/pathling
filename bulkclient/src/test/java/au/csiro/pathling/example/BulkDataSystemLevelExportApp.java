package au.csiro.pathling.example;

import au.csiro.pathling.export.BulkExportClient;
import au.csiro.pathling.export.ws.AsyncConfig;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

public class BulkDataSystemLevelExportApp {

  public static void main(String[] args) {
    // NO ERRORS
    final String fhirEndpointUrl = "https://bulk-data.smarthealthit.org/eyJlcnIiOiIiLCJwYWdlIjoxMDAwMCwiZHVyIjoxMCwidGx0IjoxNSwibSI6MSwic3R1Ijo0LCJkZWwiOjB9/fhir";

    // With transient errors in status pooling
    // final String fhirEndpointUrl = "https://bulk-data.smarthealthit.org/eyJlcnIiOiJ0cmFuc2llbnRfZXJyb3IiLCJwYWdlIjoxMDAwMCwiZHVyIjoxMCwidGx0IjoxNSwibSI6MSwic3R1Ijo0LCJkZWwiOjB9/fhir";

    // BULK Status file generation filed
    // final String fhirEndpointUrl =  "https://bulk-data.smarthealthit.org/eyJlcnIiOiJmaWxlX2dlbmVyYXRpb25fZmFpbGVkIiwicGFnZSI6MTAwMDAsImR1ciI6MTAsInRsdCI6MTUsIm0iOjEsInN0dSI6NCwiZGVsIjowfQ/fhir";
    // BULK Status some files failed to generate
    // final String fhirEndpointUrl =  "https://bulk-data.smarthealthit.org/eyJlcnIiOiJzb21lX2ZpbGVfZ2VuZXJhdGlvbl9mYWlsZWQiLCJwYWdlIjoxMDAwMCwiZHVyIjoxMCwidGx0IjoxNSwibSI6MSwic3R1Ijo0LCJkZWwiOjB9/fhir";

    // BULK FILE - File expired
    //final String fhirEndpointUrl = "https://bulk-data.smarthealthit.org/eyJlcnIiOiJmaWxlX2V4cGlyZWQiLCJwYWdlIjoxMDAwMCwiZHVyIjoxMCwidGx0IjoxNSwibSI6MSwic3R1Ijo0LCJkZWwiOjB9/fhir";

    final Instant from = Instant.parse("2020-01-01T00:00:00.000Z");
    // Bulk Export Demo Server
    final String outputDir = "target/export-" + Instant.now().toEpochMilli();

    System.out.println(
        "Exporting" + "\n from: " + fhirEndpointUrl + "\n to: " + outputDir + "\n since: " + from);

    BulkExportClient.systemBuilder()
        .withFhirEndpointUrl(fhirEndpointUrl)
        .withOutputDir(outputDir)
        .withTypes(List.of("Patient", "Condition"))
        //.withSince(from)
        .withAsyncConfig(AsyncConfig.builder().maxPoolingDelay(Duration.ofSeconds(10)).build())
        .withTimeout(Duration.ofMinutes(60))
        .build()
        .export();
  }
}
