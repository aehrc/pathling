/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.operations.bulkexport;

import static au.csiro.pathling.util.ExportOperationUtil.date;
import static au.csiro.pathling.util.ExportOperationUtil.fileInfo;
import static au.csiro.pathling.util.ExportOperationUtil.readNdjson;
import static au.csiro.pathling.util.ExportOperationUtil.req;
import static au.csiro.pathling.util.ExportOperationUtil.res;
import static au.csiro.pathling.util.ExportOperationUtil.resolveTempDirIn;
import static au.csiro.pathling.util.ExportOperationUtil.writeDetails;
import static au.csiro.pathling.util.TestConstants.RESOLVE_ENCOUNTER;
import static au.csiro.pathling.util.TestConstants.RESOLVE_PATIENT;
import static au.csiro.pathling.util.TestConstants.WAREHOUSE_PLACEHOLDER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.sink.FileInformation;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.compartment.PatientCompartmentService;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.CustomObjectDataSource;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import au.csiro.pathling.util.TestDataSetup;
import au.csiro.pathling.util.TestExportResponse;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.assertj.core.api.SoftAssertions;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.Meta;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * @author Felix Naumann
 */
@Slf4j
@Import({
  ExportOperationValidator.class,
  ExportExecutor.class,
  TestDataSetup.class,
  FhirServerTestConfiguration.class,
  PatientCompartmentService.class
})
@SpringBootUnitTest
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@Execution(ExecutionMode.CONCURRENT)
class ExportOperationExecutorTest {

  private static final String BASE = "http://localhost:8080/fhir/$export?";

  private ExportExecutor exportExecutor;

  @Autowired private SparkSession sparkSession;

  @Autowired private PathlingContext pathlingContext;

  @TempDir private Path tempDir;

  @Autowired private FhirContext fhirContext;

  @Autowired private QueryableDataSource deltaLake;

  private Path uniqueTempDir;

  @Autowired private FhirEncoders fhirEncoders;

  private IParser parser;

  @Autowired private ServerConfiguration serverConfiguration;

  @Autowired private PatientCompartmentService patientCompartmentService;

  @SuppressWarnings("unused")
  @Autowired
  private ExportResultRegistry exportResultRegistry;

  @BeforeEach
  void setup() {
    SharedMocks.resetAll();
    uniqueTempDir = tempDir.resolve(UUID.randomUUID().toString());
    try {
      Files.createDirectories(uniqueTempDir);
    } catch (final IOException e) {
      throw new RuntimeException("Failed to create unique temp dir", e);
    }
    parser = fhirContext.newJsonParser();
  }

  /**
   * Sets up the export executor with the delta lake data source. This copies test data to the temp
   * directory and should only be called by tests that actually need the delta lake data.
   */
  private void setupDeltaLakeExecutor() {
    TestDataSetup.copyTestDataToTempDir(uniqueTempDir);
    exportExecutor =
        new ExportExecutor(
            pathlingContext,
            deltaLake,
            fhirContext,
            sparkSession,
            "file://" + uniqueTempDir.toAbsolutePath(),
            serverConfiguration,
            patientCompartmentService);
  }

  @ParameterizedTest
  @MethodSource("provideUntilParameter")
  void testUntilParameter(final String until) {
    final Patient patient = new Patient();
    patient.setMeta(new Meta().setLastUpdatedElement(date("2025-01-06")));

    exportExecutor = createExecutor(patient);
    final ExportRequest req = req(BASE, date("2025-01-04"), date(until));
    final TestExportResponse actualResponse = execute(req);

    assertThat(actualResponse).isNotNull();
    assertThat(actualResponse.exportResponse().getWriteDetails().fileInfos()).hasSize(1);
  }

  private static Stream<Arguments> provideUntilParameter() {
    return Stream.of(
        arguments("2025-01-08", true),
        arguments("2025-01-04", false),
        arguments("2025-01-06", true), // same time is also fine here :)
        arguments(null, true));
  }

  @Test
  void testElementsParameterAndTypeParameterOnlyReturnRelevantResourcesAndElements()
      throws IOException {
    final Patient patient = new Patient();
    patient.addIdentifier().setValue("test");
    patient.setActive(false);
    final Observation observation = new Observation();
    observation.addIdentifier().setValue("obs-1");
    observation.setStatus(Observation.ObservationStatus.CANCELLED);

    exportExecutor = createExecutor(patient);
    final ExportRequest req =
        req(BASE, List.of("Patient"), List.of("identifier", "Observation.status"));
    final TestExportResponse actualResponse = execute(req);
    final Patient actualPatient =
        readNdjson(parser, actualResponse.getWriteDetails().fileInfos().getFirst(), Patient.class);

    assertThat(actualPatient.hasIdentifier()).isTrue();
    assertThat(actualPatient.getIdentifierFirstRep().getValue()).isEqualTo("test");

    final String wrongObservationFilepath =
        actualResponse
            .getWriteDetails()
            .fileInfos()
            .getFirst()
            .absoluteUrl()
            .replace("Patient", "Observation");
    final File observationFile = new File(URI.create(wrongObservationFilepath));
    assertThat(observationFile).doesNotExist();
  }

  @Test
  void testLocalElementInElementsParameterIsReturned() throws IOException {
    final Patient patient = new Patient();
    patient.addIdentifier().setValue("test");

    exportExecutor = createExecutor(patient);
    final ExportRequest req = req(BASE, List.of("Patient.identifier"));
    final TestExportResponse actualResponse = execute(req);
    final Patient actualPatient =
        readNdjson(parser, actualResponse.getWriteDetails().fileInfos().getFirst(), Patient.class);

    assertThat(actualPatient.hasIdentifier()).isTrue();
    assertThat(actualPatient.getIdentifierFirstRep().getValue()).isEqualTo("test");
  }

  @Test
  void testGlobalElementInElementsParameterIsReturned() throws IOException {
    final Patient patient = new Patient();
    patient.addIdentifier().setValue("test");
    final Observation observation = new Observation();
    observation.addIdentifier().setValue("test-obs");

    exportExecutor = create_exec(patient, observation);
    final ExportRequest req = req(BASE, List.of("identifier"));
    final TestExportResponse actualResponse = execute(req);
    final Patient actualPatient =
        readNdjson(parser, actualResponse.getWriteDetails().fileInfos().get(1), Patient.class);
    final Observation actualObservation =
        readNdjson(parser, actualResponse.getWriteDetails().fileInfos().get(0), Observation.class);

    assertThat(actualPatient.hasIdentifier()).isTrue();
    assertThat(actualPatient.getIdentifierFirstRep().getValue()).isEqualTo("test");
    assertThat(actualObservation.hasIdentifier()).isTrue();
    assertThat(actualObservation.getIdentifierFirstRep().getValue()).isEqualTo("test-obs");
  }

  @Test
  void testLocalAndGlobalElementsInElementsParameterAreReturned() throws IOException {
    final Patient patient = new Patient();
    patient.setActive(false);
    patient.addIdentifier().setValue("test");
    final Observation observation = new Observation();
    observation.addIdentifier().setValue("test-obs");

    exportExecutor = create_exec(patient, observation);
    final ExportRequest req = req(BASE, List.of("Patient.identifier", "active"));
    final TestExportResponse actualResponse = execute(req);
    final Patient actualPatient =
        readNdjson(parser, actualResponse.getWriteDetails().fileInfos().get(1), Patient.class);
    final Observation actualObservation =
        readNdjson(parser, actualResponse.getWriteDetails().fileInfos().get(0), Observation.class);

    assertThat(actualPatient.hasIdentifier()).isTrue();
    assertThat(actualPatient.getIdentifierFirstRep().getValue()).isEqualTo("test");
    assertThat(actualPatient.hasActive()).isTrue();
    assertThat(actualPatient.getActive()).isFalse();
    assertThat(actualObservation.hasIdentifier()).isFalse();
  }

  @Test
  void testLocalElementInElementsParameterOtherElementsAreNotReturned() throws IOException {
    final Patient patient = new Patient();
    patient.addIdentifier().setValue("test");
    patient.setActive(true);

    exportExecutor = createExecutor(patient);
    final ExportRequest req = req(BASE, List.of("Patient.identifier"));
    final TestExportResponse actualResponse = execute(req);
    final Patient actualPatient =
        readNdjson(parser, actualResponse.getWriteDetails().fileInfos().getFirst(), Patient.class);

    assertThat(actualPatient.hasActive()).isFalse();
  }

  @Test
  void testGlobalElementInElementsParameterOtherElementsAreNotReturned() throws IOException {
    // When only global elements are specified (e.g., "id"), non-requested elements should be
    // filtered out from all resource types.
    final Patient patient = new Patient();
    patient.setId("test-id");
    patient.addIdentifier().setValue("test");
    patient.setActive(true);

    exportExecutor = createExecutor(patient);
    final ExportRequest req = req(BASE, List.of("id"));
    final TestExportResponse actualResponse = execute(req);
    final Patient actualPatient =
        readNdjson(parser, actualResponse.getWriteDetails().fileInfos().getFirst(), Patient.class);

    // id and meta are mandatory and should always be returned.
    assertThat(actualPatient.hasId()).isTrue();
    assertThat(actualPatient.getIdPart()).isEqualTo("test-id");
    assertThat(actualPatient.hasMeta()).isTrue();

    // Other elements should NOT be returned.
    assertThat(actualPatient.hasIdentifier()).isFalse();
    assertThat(actualPatient.hasActive()).isFalse();
  }

  @Test
  void testMandatoryElementsAreAlwaysReturned() throws IOException {
    final Patient patient = new Patient();
    patient.setId("test-id");
    patient.setMeta(new Meta().setVersionId("1"));
    patient.addIdentifier().setValue("test");

    exportExecutor = createExecutor(patient);
    final ExportRequest req = req(BASE, List.of("Patient.identifier"));
    final TestExportResponse actualResponse = execute(req);
    final Patient actualPatient =
        readNdjson(parser, actualResponse.getWriteDetails().fileInfos().getFirst(), Patient.class);

    assertThat(actualPatient.hasId()).isTrue();
    assertThat(actualPatient.hasMeta()).isTrue();
    assertThat(actualPatient.getMeta().hasVersionId()).isTrue();
    assertThat(actualPatient.getMeta().getVersionId()).isEqualTo("1");
  }

  @Test
  void testReturnedFhirResourceHasSubsettedTag() throws IOException {
    final Patient patient = new Patient();
    patient.addIdentifier().setValue("test");
    exportExecutor = createExecutor(patient);
    final ExportRequest req = req(BASE, List.of("Patient.identifier"));
    final TestExportResponse actualResponse = execute(req);
    final Patient actualPatient =
        readNdjson(parser, actualResponse.getWriteDetails().fileInfos().getFirst(), Patient.class);

    assertThat(actualPatient.hasMeta()).isTrue();
    assertThat(actualPatient.getMeta().hasTag()).isTrue();
    assertThat(actualPatient.getMeta().getTagFirstRep().hasCode()).isTrue();
    assertThat(actualPatient.getMeta().getTagFirstRep().getCode()).isEqualTo("SUBSETTED");
  }

  @Test
  void testReturnedFhirResourceDoesNotHaveSubsettedTagIfNoElementsParameter() throws IOException {
    final Patient patient = new Patient();
    patient.addIdentifier().setValue("test");
    final Meta meta = new Meta();
    meta.addTag().setCode("other_code");
    patient.setMeta(meta);
    exportExecutor = createExecutor(patient);
    final ExportRequest req = req(BASE, List.of());
    final TestExportResponse actualResponse = execute(req);
    final Patient actualPatient =
        readNdjson(parser, actualResponse.getWriteDetails().fileInfos().getFirst(), Patient.class);

    assertThat(actualPatient.hasMeta()).isTrue();
    assertThat(actualPatient.getMeta().hasTag()).isTrue();
    final Set<String> tags =
        actualPatient.getMeta().getTag().stream().map(Coding::getCode).collect(Collectors.toSet());
    assertThat(tags).isNotEmpty().doesNotContain("SUBSETTED");
  }

  @Test
  void testExistingMetaTagRemainsWhenSubsettedTagIsAdded() throws IOException {
    final Patient patient = new Patient();
    patient.addIdentifier().setValue("test");
    final Meta meta = new Meta();
    meta.addTag().setCode("other_code");
    patient.setMeta(meta);
    exportExecutor = createExecutor(patient);
    final ExportRequest req = req(BASE, List.of("Patient.identifier"));
    final TestExportResponse actualResponse = execute(req);
    final Patient actualPatient =
        readNdjson(parser, actualResponse.getWriteDetails().fileInfos().getFirst(), Patient.class);

    assertThat(actualPatient.hasMeta()).isTrue();
    assertThat(actualPatient.getMeta().hasTag()).isTrue();
    final Set<String> tags =
        actualPatient.getMeta().getTag().stream().map(Coding::getCode).collect(Collectors.toSet());
    assertThat(tags).isNotEmpty().contains("other_code");
  }

  @Test
  void testOtherExistingMetaValuesRemainWhenSubsettedTagIsAdded() throws IOException {
    final Patient patient = new Patient();
    patient.addIdentifier().setValue("test");
    patient.setMeta(new Meta().setSource("source1"));
    exportExecutor = createExecutor(patient);
    final ExportRequest req = req(BASE, List.of("Patient.identifier"));
    final TestExportResponse actualResponse = execute(req);
    final Patient actualPatient =
        readNdjson(parser, actualResponse.getWriteDetails().fileInfos().getFirst(), Patient.class);

    assertThat(actualPatient.hasMeta()).isTrue();
    assertThat(actualPatient.getMeta().hasSource()).isTrue();
    assertThat(actualPatient.getMeta().getSource()).isEqualTo("source1");
  }

  private ExportExecutor createExecutor(final IBaseResource resource) {
    return create_exec(List.of(resource));
  }

  private ExportExecutor create_exec(final IBaseResource... resources) {
    return create_exec(Arrays.asList(resources));
  }

  private ExportExecutor create_exec(final List<IBaseResource> resources) {
    final CustomObjectDataSource objectDataSource =
        new CustomObjectDataSource(sparkSession, pathlingContext, fhirEncoders, resources);
    exportExecutor =
        new ExportExecutor(
            pathlingContext,
            objectDataSource,
            fhirContext,
            sparkSession,
            "file://" + uniqueTempDir.toAbsolutePath(),
            serverConfiguration,
            patientCompartmentService);
    return exportExecutor;
  }

  @ParameterizedTest
  @MethodSource("provideSinceParameters")
  void testSinceParameter(
      final InstantType patientLastUpdated,
      final ExportRequest exportRequest,
      ExportResponse expectedExportResponse) {
    final Patient patient = new Patient();
    patient.setMeta(new Meta().setLastUpdatedElement(patientLastUpdated));
    final CustomObjectDataSource objectDataSource =
        new CustomObjectDataSource(sparkSession, pathlingContext, fhirEncoders, List.of(patient));
    exportExecutor =
        new ExportExecutor(
            pathlingContext,
            objectDataSource,
            fhirContext,
            sparkSession,
            "file://" + uniqueTempDir.toAbsolutePath(),
            serverConfiguration,
            patientCompartmentService);

    final TestExportResponse actualExportResponse = execute(exportRequest);
    expectedExportResponse =
        resolveTempDirIn(expectedExportResponse, uniqueTempDir, actualExportResponse.fakeJobId());
    assertResponse(actualExportResponse.exportResponse(), expectedExportResponse);
  }

  private static Stream<Arguments> provideSinceParameters() {
    final InstantType patientLastUpdated = date("2024-01-06");

    final String base = "http://localhost:8080/fhir/$export?";
    final ExportRequest req1 =
        req(base, ExportOutputFormat.NDJSON, date("2024-01-05"), List.of("Patient"));
    final ExportResponse res1 =
        res(req1, writeDetails(fileInfo("Patient", RESOLVE_PATIENT.apply(WAREHOUSE_PLACEHOLDER))));

    final ExportRequest req2 =
        req(base, ExportOutputFormat.NDJSON, date("2024-01-07"), List.of("Patient"));
    final ExportResponse res2 =
        res(req2, writeDetails(fileInfo("Patient", RESOLVE_PATIENT.apply(WAREHOUSE_PLACEHOLDER))));

    return Stream.of(
        arguments(patientLastUpdated, req1, res1),
        arguments(patientLastUpdated, req2, res2),
        arguments(null, req1, res1),
        arguments(
            null,
            req2,
            res(
                req2,
                writeDetails(fileInfo("Patient", RESOLVE_PATIENT.apply(WAREHOUSE_PLACEHOLDER))))));
  }

  @ParameterizedTest
  @MethodSource("provideExportExecutorValues")
  void testExportExecutor(
      final ExportRequest exportRequest, final ExportResponse expectedExportResponse) {
    setupDeltaLakeExecutor();
    final TestExportResponse actualExportResponse = execute(exportRequest);
    final ExportResponse resolvedExportResponse =
        resolveTempDirIn(expectedExportResponse, uniqueTempDir, actualExportResponse.fakeJobId());
    assertResponse(actualExportResponse.exportResponse(), resolvedExportResponse);
  }

  private static Stream<Arguments> provideExportExecutorValues() {
    final String warehouseUrl = WAREHOUSE_PLACEHOLDER;
    final String base = "http://localhost:8080/fhir/$export?";
    final InstantType now = InstantType.now();

    final ExportRequest req1 = req(base, ExportOutputFormat.NDJSON, now, List.of("Patient"));
    final ExportResponse res1 =
        res(req1, writeDetails(fileInfo("Patient", RESOLVE_PATIENT.apply(warehouseUrl))));

    final ExportRequest req2 = req(base, ExportOutputFormat.NDJSON, now, List.of("Encounter"));
    final ExportResponse res2 =
        res(req2, writeDetails(fileInfo("Encounter", RESOLVE_ENCOUNTER.apply(warehouseUrl))));

    final ExportRequest req3 =
        req(base, ExportOutputFormat.NDJSON, now, List.of("Encounter", "Patient"));
    final ExportResponse res3 =
        res(
            req3,
            writeDetails(
                fileInfo("Encounter", RESOLVE_ENCOUNTER.apply(warehouseUrl)),
                fileInfo("Patient", RESOLVE_PATIENT.apply(warehouseUrl))));

    final ExportRequest req4 = req(base, ExportOutputFormat.NDJSON, now, List.of());
    final ExportResponse res4 =
        res(
            req4,
            writeDetails(
                fileInfo("Encounter", RESOLVE_ENCOUNTER.apply(warehouseUrl)),
                fileInfo("Patient", RESOLVE_PATIENT.apply(warehouseUrl))));

    return Stream.of(
        arguments(req1, res1), arguments(req2, res2), arguments(req3, res3), arguments(req4, res4));
  }

  private static void assertResponse(final ExportResponse actual, final ExportResponse expected) {
    final SoftAssertions softly = new SoftAssertions();
    softly.assertThat(actual.getKickOffRequestUrl()).isEqualTo(expected.getKickOffRequestUrl());
    softly.assertThat(actual.getWriteDetails()).isNotNull();
    softly
        .assertThat(actual.getWriteDetails().fileInfos())
        .extracting(FileInformation::fhirResourceType, FileInformation::absoluteUrl)
        .containsAnyElementsOf(
            expected.getWriteDetails().fileInfos().stream()
                .map(fi -> tuple(fi.fhirResourceType(), fi.absoluteUrl()))
                .toList());
    softly.assertAll();
  }

  /**
   * Use for tests only where it does not matter in which subdirectory the ndjson is written to.
   *
   * @param exportRequest Information about the export request.
   * @return Information about the export response.
   */
  public TestExportResponse execute(final ExportRequest exportRequest) {
    final UUID uuid = UUID.randomUUID();
    return new TestExportResponse(uuid, exportExecutor.execute(exportRequest, uuid.toString()));
  }
}
