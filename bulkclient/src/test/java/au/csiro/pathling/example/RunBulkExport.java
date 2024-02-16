/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.example;

import au.csiro.pathling.export.BulkExportClient;
import au.csiro.pathling.export.fhir.Reference;
import au.csiro.pathling.export.ws.BulkExportRequest.PatientLevel;
import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.List;

/**
 * Example of running the Bulk Export Client
 */
public class RunBulkExport {


  public static void runSystemLevel() throws Exception {

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

    BulkExportClient.builder()
        .withFhirEndpointUrl(fhirEndpointUrl)
        .withOutputDir(outputDir)
        .withType(List.of("Patient", "Condition"))
        .withSince(from)
        .build()
        .export();
  }


  public static void runPatientLevel() throws Exception {

    // NO ERRORS
    final String fhirEndpointUrl = "https://bulk-data.smarthealthit.org/eyJlcnIiOiIiLCJwYWdlIjoxMDAwMCwiZHVyIjoxMCwidGx0IjoxNSwibSI6MSwic3R1Ijo0LCJkZWwiOjB9/fhir";

    final Instant from = Instant.parse("2020-01-01T00:00:00.000Z");
    // Bulk Export Demo Server
    final String outputDir = "target/export-" + Instant.now().toEpochMilli();
    System.out.println(
        "Exporting" + "\n from: " + fhirEndpointUrl + "\n to: " + outputDir + "\n since: " + from);

    BulkExportClient.builder()
        .withFhirEndpointUrl(fhirEndpointUrl)
        .withOutputDir(outputDir)
        .withType(List.of("Patient", "Condition"))
        .withSince(from)
        .withOperation(new PatientLevel())
        .withPatient(Reference.of("Patient/6c5d9ca9-54d7-42f5-bfae-a7c19cd217f2"))
        .withPatient(Reference.of("Patient/538a9a4e-8437-47d3-8c01-1a17dca8f0be"))
        .withPatient(Reference.of("Patient/fbfec681-d357-4b28-b1d2-5db6434c7846"))
        .build()
        .export();
  }
  
  public static void main(@Nonnull final String[] args) throws Exception {
    runSystemLevel();
  }
}
