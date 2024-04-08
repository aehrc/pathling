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

import au.csiro.pathling.config.AuthConfiguration;
import au.csiro.pathling.export.BulkExportClient;
import au.csiro.pathling.export.fhir.Reference;
import au.csiro.pathling.export.ws.AsyncConfig;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.commons.io.IOUtils;

/**
 * Example of running the Bulk Export Client
 */
public class RunBulkExport {

  /**
   * Run the Bulk Export Client with Cerner.
   */
  public static void runCerner() {
    final String fhirEndpointUrl = "https://fhir-ehr-code.cerner.com/r4/ec2458f2-1e24-41c8-b71b-0e701af7583d";
    final String clientSecret = System.getProperty("pszul.cerner.clientSecret");
    System.out.println("client secret: " + clientSecret);

    final Instant from = Instant.parse("2020-01-01T00:00:00.000Z");
    // Bulk Export Demo Server
    final String outputDir = "target/export-" + Instant.now().toEpochMilli();

    System.out.println(
        "Exporting" + "\n from: " + fhirEndpointUrl + "\n to: " + outputDir + "\n since: " + from);

    final AuthConfiguration smartCDR = AuthConfiguration.builder()
        .enabled(true)
        .tokenEndpoint(
            "https://authorization.cerner.com/tenants/ec2458f2-1e24-41c8-b71b-0e701af7583d/protocols/oauth2/profiles/smart-v1/token")
        .clientId("4ccde388-534e-482b-b6ca-c55571432c08")
        .clientSecret(clientSecret)
        .scope("system/Patient.read")
        .build();

    BulkExportClient.groupBuilder("11ec-d16a-b40370f8-9d31-577f11a339c5")
        .withAuthConfig(smartCDR)
        .withFhirEndpointUrl(fhirEndpointUrl)
        .withOutputDir(outputDir)
        .withTypes(List.of("Patient"))
        //.withSince(Instant.now().minus(Duration.ofDays(700)))
        .withTimeout(Duration.ofMinutes(5))
        .build()
        .export();
  }


  /**
   * Run the Bulk Export Client with Cerner.
   *
   * @throws Exception if an error occurs
   */
  public static void runCernerAssymetric() throws Exception {
    final String fhirEndpointUrl = "https://fhir-ehr-code.cerner.com/r4/ec2458f2-1e24-41c8-b71b-0e701af7583d";

    final Instant from = Instant.parse("2020-01-01T00:00:00.000Z");
    // Bulk Export Demo Server
    final String outputDir = "target/export-" + Instant.now().toEpochMilli();

    System.out.println(
        "Exporting" + "\n from: " + fhirEndpointUrl + "\n to: " + outputDir + "\n since: " + from);

    final String privateKeyJWK = IOUtils.toString(
        RunBulkExport.class.getClassLoader().getResourceAsStream(
            "auth/bulk_rs384_priv_jwk.json"));
    System.out.println("privateKeyJWK: " + privateKeyJWK);

    final AuthConfiguration smartCDR = AuthConfiguration.builder()
        .enabled(true)
        .tokenEndpoint(
            "https://authorization.cerner.com/tenants/ec2458f2-1e24-41c8-b71b-0e701af7583d/protocols/oauth2/profiles/smart-v1/token")
        .clientId("4ccde388-534e-482b-b6ca-c55571432c08")
        .privateKeyJWK(privateKeyJWK)
        .scope("system/Patient.read")
        .build();

    BulkExportClient.groupBuilder("11ec-d16a-b40370f8-9d31-577f11a339c5")
        .withAuthConfig(smartCDR)
        .withFhirEndpointUrl(fhirEndpointUrl)
        .withOutputDir(outputDir)
        .withTypes(List.of("Patient"))
        //.withSince(Instant.now().minus(Duration.ofDays(700)))
        .withTimeout(Duration.ofMinutes(5))
        .build()
        .export();
  }

  /**
   * Run the Bulk Export Client with Cerner.
   */
  public static void runSystemLevelCdr() {

    // NO ERRORS
    final String fhirEndpointUrl = "https://aehrc-cdr.cc/fhir_r4";
    final String clientSecret = System.getProperty("pszul.clientSecret");
    System.out.println("client secret: " + clientSecret);

    final Instant from = Instant.parse("2020-01-01T00:00:00.000Z");
    // Bulk Export Demo Server
    final String outputDir = "target/export-" + Instant.now().toEpochMilli();

    System.out.println(
        "Exporting" + "\n from: " + fhirEndpointUrl + "\n to: " + outputDir + "\n since: " + from);

    final AuthConfiguration smartCDR = AuthConfiguration.builder()
        .enabled(true)
        .tokenEndpoint("https://aehrc-cdr.cc/smartsec_r4/oauth/token")
        .clientId("pathling-bulk-client")
        .clientSecret(clientSecret)
        .scope("system/*.read")
        .build();

    BulkExportClient.systemBuilder()
        .withAuthConfig(smartCDR)
        .withFhirEndpointUrl(fhirEndpointUrl)
        .withOutputDir(outputDir)
        .withTypes(List.of("MedicationRequest"))
        //.withSince(Instant.now().minus(Duration.ofDays(700)))
        .withAsyncConfig(AsyncConfig.builder().maxPoolingDelay(Duration.ofSeconds(10)).build())
        .withTimeout(Duration.ofMinutes(60))
        .build()
        .export();
  }


  /**
   * Run the Bulk Export Client with Cerner.
   */
  public static void runSystemLevel() {

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

  /**
   * Run the Bulk Export Client with Cerner.
   *
   * @throws Exception if an error occurs
   */
  public static void runSystemLevelWithAuthRS384() throws Exception {

    // NO ERRORS
    final String fhirEndpointUrl = "https://bulk-data.smarthealthit.org/eyJlcnIiOiIiLCJwYWdlIjoxMDAwMCwiZHVyIjoxMCwidGx0IjoxNSwibSI6MSwic3R1Ijo0LCJkZWwiOjAsInNlY3VyZSI6MX0/fhir";
    final Instant from = Instant.parse("2020-01-01T00:00:00.000Z");
    // Bulk Export Demo Server
    final String outputDir = "target/export-" + Instant.now().toEpochMilli();

    System.out.println(
        "Exporting" + "\n from: " + fhirEndpointUrl + "\n to: " + outputDir + "\n since: " + from);

    final String clientId = IOUtils.toString(
        RunBulkExport.class.getClassLoader().getResourceAsStream("auth/bulk_rs384_clientId.txt"));
    final String privateKeyJWK = IOUtils.toString(
        RunBulkExport.class.getClassLoader().getResourceAsStream("auth/bulk_rs384_priv_jwk.json"));

    System.out.println("clientId: " + clientId);
    System.out.println("privateKeyJWK: " + privateKeyJWK);

    AuthConfiguration authConfiguration = AuthConfiguration.builder()
        .enabled(true)
        .useSMART(true)
        .clientId(clientId)
        .privateKeyJWK(privateKeyJWK)
        .scope("system/*.read")
        .tokenExpiryTolerance(59)
        .build();

    BulkExportClient.systemBuilder()
        .withFhirEndpointUrl(fhirEndpointUrl)
        .withOutputDir(outputDir)
        .withTypes(List.of("Patient", "Condition"))
        //.withSince(from)
        .withAsyncConfig(AsyncConfig.builder().maxPoolingDelay(Duration.ofSeconds(10)).build())
        .withTimeout(Duration.ofMinutes(60))
        .withAuthConfig(authConfiguration)
        .build()
        .export();
  }


  /**
   * Run the Bulk Export Client with Cerner.
   *
   * @throws Exception if an error occurs
   */
  public static void runSystemLevelWithAuthES384() throws Exception {

    // NO ERRORS
    final String fhirEndpointUrl = "https://bulk-data.smarthealthit.org/eyJlcnIiOiIiLCJwYWdlIjoxMDAwMCwiZHVyIjoxMCwidGx0IjoxNSwibSI6MSwic3R1Ijo0LCJkZWwiOjAsInNlY3VyZSI6MX0/fhir";
    final Instant from = Instant.parse("2020-01-01T00:00:00.000Z");
    // Bulk Export Demo Server
    final String outputDir = "target/export-" + Instant.now().toEpochMilli();

    System.out.println(
        "Exporting" + "\n from: " + fhirEndpointUrl + "\n to: " + outputDir + "\n since: " + from);

    final String clientId = IOUtils.toString(
        RunBulkExport.class.getClassLoader().getResourceAsStream("auth/bulk_es384_clientId.txt"));
    final String privateKeyJWK = IOUtils.toString(
        RunBulkExport.class.getClassLoader().getResourceAsStream(
            "auth/bulk_es384_priv_jwk.json"));

    System.out.println("clientId: " + clientId);
    System.out.println("privateKeyJWK: " + privateKeyJWK);

    AuthConfiguration authConfiguration = AuthConfiguration.builder()
        .enabled(true)
        .tokenEndpoint("https://bulk-data.smarthealthit.org/auth/token")
        .clientId(clientId)
        .privateKeyJWK(privateKeyJWK)
        .scope("system/*.read")
        .build();

    BulkExportClient.systemBuilder()
        .withFhirEndpointUrl(fhirEndpointUrl)
        .withOutputDir(outputDir)
        .withTypes(List.of("Patient", "Condition"))
        //.withSince(from)
        .withAsyncConfig(AsyncConfig.builder().maxPoolingDelay(Duration.ofSeconds(10)).build())
        .withTimeout(Duration.ofMinutes(60))
        .withAuthConfig(authConfiguration)
        .build()
        .export();
  }


  /**
   * Run the Bulk Export Client with Cerner.
   */
  public static void runPatientLevel() {

    // NO ERRORS
    final String fhirEndpointUrl = "https://bulk-data.smarthealthit.org/eyJlcnIiOiIiLCJwYWdlIjoxMDAwMCwiZHVyIjoxMCwidGx0IjoxNSwibSI6MSwic3R1Ijo0LCJkZWwiOjB9/fhir";

    final Instant from = Instant.parse("2020-01-01T00:00:00.000Z");
    // Bulk Export Demo Server
    final String outputDir = "target/export-" + Instant.now().toEpochMilli();
    System.out.println(
        "Exporting" + "\n from: " + fhirEndpointUrl + "\n to: " + outputDir + "\n since: " + from);

    BulkExportClient.patientBuilder()
        .withFhirEndpointUrl(fhirEndpointUrl)
        .withOutputDir(outputDir)
        .withType("Patient")
        .withType("Condition")
        .withSince(from)
        .withPatient(Reference.of("Patient/6c5d9ca9-54d7-42f5-bfae-a7c19cd217f2"))
        .withPatient(Reference.of("Patient/538a9a4e-8437-47d3-8c01-1a17dca8f0be"))
        .withPatient(Reference.of("Patient/fbfec681-d357-4b28-b1d2-5db6434c7846"))
        .build()
        .export();
  }

  /**
   * Run the Bulk Export Client with Cerner.
   *
   * @param args the command line arguments
   * @throws Exception if an error occurs
   */
  public static void main(@Nonnull final String[] args) throws Exception {
    runSystemLevelWithAuthRS384();
  }
}
