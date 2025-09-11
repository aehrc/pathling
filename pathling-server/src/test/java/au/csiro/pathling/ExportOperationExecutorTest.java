package au.csiro.pathling;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.export.*;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.sink.DataSink;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.CustomObjectDataSource;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import au.csiro.pathling.util.TestDataSetup;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.assertj.core.api.SoftAssertions;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

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

import static au.csiro.pathling.util.ExportOperationUtil.*;
import static au.csiro.pathling.util.TestConstants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hl7.fhir.r4.model.Enumerations.ResourceType;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * @author Felix Naumann
 */
@Slf4j
@Import({
        ExportOperationValidator.class,
        ExportExecutor.class,
        PathlingContext.class,
        TestDataSetup.class,
        FhirServerTestConfiguration.class
})
@SpringBootUnitTest
//@TestInstance(TestInstance.Lifecycle.PER_METHOD)
//@Execution(ExecutionMode.CONCURRENT)
//@ContextConfiguration(initializers = ExportOperationExecutorTest.Initializer.class)
class ExportOperationExecutorTest {

    private static String BASE = "http://localhost:8080/fhir/$export?";

    private ExportExecutor exportExecutor;

    @Autowired
    private TestDataSetup testDataSetup;

    @Autowired
    private SparkSession sparkSession;
    @Autowired
    private PathlingContext pathlingContext;

    @TempDir
    private Path tempDir;
    @Autowired
    private FhirContext fhirContext;
    @Autowired
    private QueryableDataSource deltaLake;

    private Path uniqueTempDir;
    @Autowired
    private FhirEncoders fhirEncoders;

    private IParser parser;

    @BeforeEach
    void setup() {
        SharedMocks.resetAll();
        exportExecutor = new ExportExecutor(
                pathlingContext,
                deltaLake,
                fhirContext
        );

        uniqueTempDir = tempDir.resolve(UUID.randomUUID().toString());
        try {
            Files.createDirectories(uniqueTempDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create unique temp dir", e);
        }

        testDataSetup.copyTestDataToTempDir(uniqueTempDir);
        exportExecutor.setWarehouseUrl("file://" + uniqueTempDir.toAbsolutePath());

        parser = fhirContext.newJsonParser();
    }
    
    @ParameterizedTest
    @MethodSource("provide_until_param")
    void test_until_param(String until, boolean returnedResource) {
      Patient patient = new Patient();
      patient.setMeta(new Meta().setLastUpdatedElement(date("2025-01-06")));

      exportExecutor = create_exec(patient);
      ExportRequest req = req(BASE, date("2025-01-04"), date(until));
      ExportResponse actualResponse = exportExecutor.execute(req);
      
      assertThat(actualResponse).isNotNull();
      assertThat(actualResponse.getWriteDetails().fileInfos()).hasSize(1);
      long expectedCount = returnedResource ? 1 : 0;
      assertThat(actualResponse.getWriteDetails().fileInfos().get(0).count()).isEqualTo(expectedCount);

    }
    
    private static Stream<Arguments> provide_until_param() {
      return Stream.of(
          arguments("2025-01-08", true),
          arguments("2025-01-04", false),
          arguments("2025-01-06", true), // same time is also fine here :)
          arguments(null, true)
      );
    }

    @Test
    void elements_param_and_type_param_only_return_relevant_resources_and_elements() throws IOException {
        Patient patient = new Patient();
        patient.addIdentifier().setValue("test");
        patient.setActive(false);
        Observation observation = new Observation();
        observation.addIdentifier().setValue("obs-1");
        observation.setStatus(Observation.ObservationStatus.CANCELLED);

        exportExecutor = create_exec(patient);
        ExportRequest req = req(BASE, List.of(ResourceType.PATIENT), List.of("identifier", "Observation.status"));
        ExportResponse actualResponse = exportExecutor.execute(req);
        Patient actualPatient = read_ndjson(parser, actualResponse.getWriteDetails().fileInfos().get(0), Patient.class);
        //Patient actualObservation = read_ndjson(actualResponse.getWriteDetails().fileInfos().get(1), Patient.class);

        assertThat(actualPatient.hasIdentifier()).isTrue();
        assertThat(actualPatient.getIdentifierFirstRep().getValue()).isEqualTo("test");

        String wrongObservationFilepath = actualResponse.getWriteDetails().fileInfos().get(0).absoluteUrl().replace("Patient", "Observation");
        File observationFile = new File(URI.create(wrongObservationFilepath));
        assertThat(observationFile).doesNotExist();
    }

    @Test
    void local_element_in_elements_param_is_returned() throws IOException {
        Patient patient = new Patient();
        patient.addIdentifier().setValue("test");

        exportExecutor = create_exec(patient);
        ExportRequest req = req(BASE, List.of("Patient.identifier"));
        ExportResponse actualResponse = exportExecutor.execute(req);
        Patient actualPatient = read_ndjson(parser, actualResponse.getWriteDetails().fileInfos().get(0), Patient.class);

        assertThat(actualPatient.hasIdentifier()).isTrue();
        assertThat(actualPatient.getIdentifierFirstRep().getValue()).isEqualTo("test");
    }

    @Test
    void global_element_in_elements_param_is_returned() throws IOException {
        Patient patient = new Patient();
        patient.addIdentifier().setValue("test");
        Observation observation = new Observation();
        observation.addIdentifier().setValue("test-obs");

        exportExecutor = create_exec(patient, observation);
        ExportRequest req = req(BASE, List.of("identifier"));
        ExportResponse actualResponse = exportExecutor.execute(req);
        Patient actualPatient = read_ndjson(parser, actualResponse.getWriteDetails().fileInfos().get(1), Patient.class);
        Observation actualObservation = read_ndjson(parser, actualResponse.getWriteDetails().fileInfos().get(0), Observation.class);

        assertThat(actualPatient.hasIdentifier()).isTrue();
        assertThat(actualPatient.getIdentifierFirstRep().getValue()).isEqualTo("test");
        assertThat(actualObservation.hasIdentifier()).isTrue();
        assertThat(actualObservation.getIdentifierFirstRep().getValue()).isEqualTo("test-obs");
    }

    @Test
    void local_and_global_elements_in_elements_param_are_returned() throws IOException {
        Patient patient = new Patient();
        patient.setActive(false);
        patient.addIdentifier().setValue("test");
        Observation observation = new Observation();
        observation.addIdentifier().setValue("test-obs");

        exportExecutor = create_exec(patient, observation);
        ExportRequest req = req(BASE, List.of("Patient.identifier", "active"));
        ExportResponse actualResponse = exportExecutor.execute(req);
        Patient actualPatient = read_ndjson(parser, actualResponse.getWriteDetails().fileInfos().get(1), Patient.class);
        Observation actualObservation = read_ndjson(parser, actualResponse.getWriteDetails().fileInfos().get(0), Observation.class);

        assertThat(actualPatient.hasIdentifier()).isTrue();
        assertThat(actualPatient.getIdentifierFirstRep().getValue()).isEqualTo("test");
        assertThat(actualPatient.hasActive()).isTrue();
        assertThat(actualPatient.getActive()).isFalse();
        assertThat(actualObservation.hasIdentifier()).isFalse();
    }

    @Test
    void local_element_in_elements_param_other_elements_are_not_returned() throws IOException {
        Patient patient = new Patient();
        patient.addIdentifier().setValue("test");
        patient.setActive(true);
        
        exportExecutor = create_exec(patient);
        ExportRequest req = req(BASE, List.of("Patient.identifier"));
        ExportResponse actualResponse = exportExecutor.execute(req);
        Patient actualPatient = read_ndjson(parser, actualResponse.getWriteDetails().fileInfos().get(0), Patient.class);

        assertThat(actualPatient.hasActive()).isFalse();
    }

    @Test
    void mandatory_elements_are_always_returned() throws IOException {
        Patient patient = new Patient();
        patient.setId("test-id");
        patient.setMeta(new Meta().setVersionId("1"));
        patient.addIdentifier().setValue("test");
        
        exportExecutor = create_exec(patient);
        ExportRequest req = req(BASE, List.of("Patient.identifier"));
        ExportResponse actualResponse = exportExecutor.execute(req);
        Patient actualPatient = read_ndjson(parser, actualResponse.getWriteDetails().fileInfos().get(0), Patient.class);

        assertThat(actualPatient.hasId()).isTrue();
        // don't actually assert the content because it's modified by pathling (due to history tracking)
        // assertThat(actualPatient.getId()).isEqualTo("test-id");
        assertThat(actualPatient.hasMeta()).isTrue();
        assertThat(actualPatient.getMeta().hasVersionId()).isTrue();
        assertThat(actualPatient.getMeta().getVersionId()).isEqualTo("1");
    }

    @Test
    void returned_fhir_resource_has_subsetted_tag() throws IOException {
        Patient patient = new Patient();
        patient.addIdentifier().setValue("test");
        exportExecutor = create_exec(patient);
        ExportRequest req = req(BASE, List.of("Patient.identifier"));
        ExportResponse actualResponse = exportExecutor.execute(req);
        Patient actualPatient = read_ndjson(parser, actualResponse.getWriteDetails().fileInfos().get(0), Patient.class);

        assertThat(actualPatient.hasMeta()).isTrue();
        assertThat(actualPatient.getMeta().hasTag()).isTrue();
        assertThat(actualPatient.getMeta().getTagFirstRep().hasCode()).isTrue();
        assertThat(actualPatient.getMeta().getTagFirstRep().getCode()).isEqualTo("SUBSETTED");
    }

    @Test
    void returned_fhir_resource_does_not_have_subsetted_tag_if_no_elements_param() throws IOException {
        Patient patient = new Patient();
        patient.addIdentifier().setValue("test");
        Meta meta = new Meta();
        meta.addTag().setCode("other_code");
        patient.setMeta(meta);
        exportExecutor = create_exec(patient);
        ExportRequest req = req(BASE, List.of());
        ExportResponse actualResponse = exportExecutor.execute(req);
        Patient actualPatient = read_ndjson(parser, actualResponse.getWriteDetails().fileInfos().get(0), Patient.class);

        assertThat(actualPatient.hasMeta()).isTrue();
        assertThat(actualPatient.getMeta().hasTag()).isTrue();
        Set<String> tags = actualPatient.getMeta().getTag().stream().map(Coding::getCode).collect(Collectors.toSet());
        assertThat(tags).isNotEmpty().doesNotContain("SUBSETTED");
    }

    @Test
    void existing_meta_tag_remains_when_subsetted_tag_is_added() throws IOException {
        Patient patient = new Patient();
        patient.addIdentifier().setValue("test");
        Meta meta = new Meta();
        meta.addTag().setCode("other_code");
        patient.setMeta(meta);
        exportExecutor = create_exec(patient);
        ExportRequest req = req(BASE, List.of("Patient.identifier"));
        ExportResponse actualResponse = exportExecutor.execute(req);
        Patient actualPatient = read_ndjson(parser, actualResponse.getWriteDetails().fileInfos().get(0), Patient.class);

        assertThat(actualPatient.hasMeta()).isTrue();
        assertThat(actualPatient.getMeta().hasTag()).isTrue();
        Set<String> tags = actualPatient.getMeta().getTag().stream().map(Coding::getCode).collect(Collectors.toSet());
        assertThat(tags).isNotEmpty().contains("other_code");
    }

    @Test
    void other_existing_meta_values_remain_when_subsetted_tag_is_added() throws IOException {
        Patient patient = new Patient();
        patient.addIdentifier().setValue("test");
        patient.setMeta(new Meta().setSource("source1"));
        exportExecutor = create_exec(patient);
        ExportRequest req = req(BASE, List.of("Patient.identifier"));
        ExportResponse actualResponse = exportExecutor.execute(req);
        Patient actualPatient = read_ndjson(parser, actualResponse.getWriteDetails().fileInfos().get(0), Patient.class);

        assertThat(actualPatient.hasMeta()).isTrue();
        assertThat(actualPatient.getMeta().hasSource()).isTrue();
        assertThat(actualPatient.getMeta().getSource()).isEqualTo("source1");
    }

    private ExportExecutor create_exec(IBaseResource resource) {
        return create_exec(List.of(resource));
    }

    private ExportExecutor create_exec(IBaseResource... resources) {
        return create_exec(Arrays.asList(resources));
    }

    private ExportExecutor create_exec(List<IBaseResource> resources) {
        CustomObjectDataSource objectDataSource = new CustomObjectDataSource(sparkSession, pathlingContext, fhirEncoders, resources);
        exportExecutor = new ExportExecutor(pathlingContext, objectDataSource, fhirContext);
        exportExecutor.setWarehouseUrl("file://" + uniqueTempDir.toAbsolutePath());
        return exportExecutor;
    }

    @ParameterizedTest
    @MethodSource("provide_since_params")
    void test_since_parameter(InstantType patientLastUpdated, ExportRequest exportRequest, ExportResponse expectedExportResponse) {
        Patient patient = new Patient();
        patient.setMeta(new Meta().setLastUpdatedElement(patientLastUpdated));
        CustomObjectDataSource objectDataSource = new CustomObjectDataSource(sparkSession, pathlingContext, fhirEncoders, List.of(patient));
        exportExecutor = new ExportExecutor(
                pathlingContext,
                objectDataSource,
                fhirContext
        );
        exportExecutor.setWarehouseUrl("file://" + uniqueTempDir.toAbsolutePath());

        expectedExportResponse = resolveTempDirIn(expectedExportResponse, uniqueTempDir);
        ExportResponse actualExportResponse = exportExecutor.execute(exportRequest);
        assert_response(actualExportResponse, expectedExportResponse);
    }

    private static Stream<Arguments> provide_since_params() {
        InstantType patientLastUpdated = date("2024-01-06");

        String base = "http://localhost:8080/fhir/$export?";
        ExportRequest req1 = req(base, ExportOutputFormat.ND_JSON, date("2024-01-05"), List.of(ResourceType.PATIENT));
        ExportResponse res1 = res(req1, write_details(fi("Patient", RESOLVE_PATIENT.apply(WAREHOUSE_PLACEHOLDER), 1)));

        ExportRequest req2 = req(base, ExportOutputFormat.ND_JSON, date("2024-01-07"), List.of(ResourceType.PATIENT));
        ExportResponse res2 = res(req2, write_details(fi("Patient", RESOLVE_PATIENT.apply(WAREHOUSE_PLACEHOLDER), 0)));

        return Stream.of(
                arguments(patientLastUpdated, req1, res1),
                arguments(patientLastUpdated, req2, res2),
                arguments(null, req1, res1),
                arguments(null, req2, res(req2, write_details(fi("Patient", RESOLVE_PATIENT.apply(WAREHOUSE_PLACEHOLDER), 1))))
        );
    }

    @ParameterizedTest
    @MethodSource("provide_export_executor_values")
    //@DirtiesContext(methodMode = DirtiesContext.MethodMode.BEFORE_METHOD)
    void test_export_executor(ExportRequest exportRequest, ExportResponse expectedExportResponse) {
        expectedExportResponse = resolveTempDirIn(expectedExportResponse, uniqueTempDir);
        ExportResponse actualExportResponse = exportExecutor.execute(exportRequest);
        assert_response(actualExportResponse, expectedExportResponse);
    }

    private static Stream<Arguments> provide_export_executor_values() {
        String warehouseUrl = WAREHOUSE_PLACEHOLDER;
        String base = "http://localhost:8080/fhir/$export?";
        InstantType now = InstantType.now();

        ExportRequest req1 = req(base, ExportOutputFormat.ND_JSON, now, List.of(ResourceType.PATIENT));
        ExportResponse res1 = res(req1, write_details(fi("Patient", RESOLVE_PATIENT.apply(warehouseUrl), PATIENT_COUNT)));

        ExportRequest req2 = req(base, ExportOutputFormat.ND_JSON, now, List.of(ResourceType.ENCOUNTER));
        ExportResponse res2 = res(req2, write_details(fi("Encounter", RESOLVE_ENCOUNTER.apply(warehouseUrl), ENCOUNTER_COUNT)));

        ExportRequest req3 = req(base, ExportOutputFormat.ND_JSON, now, List.of(ResourceType.ENCOUNTER, ResourceType.PATIENT));
        ExportResponse res3 = res(req3, write_details(
                fi("Encounter", RESOLVE_ENCOUNTER.apply(warehouseUrl), ENCOUNTER_COUNT),
                fi("Patient", RESOLVE_PATIENT.apply(warehouseUrl), PATIENT_COUNT)));

        ExportRequest req4 = req(base, ExportOutputFormat.ND_JSON, now, List.of());
        ExportResponse res4 = res(req4, write_details(
                fi("Encounter", RESOLVE_ENCOUNTER.apply(warehouseUrl), ENCOUNTER_COUNT),
                fi("Patient", RESOLVE_PATIENT.apply(warehouseUrl), PATIENT_COUNT)));

        return Stream.of(
                arguments(req1, res1),
                arguments(req2, res2),
                arguments(req3, res3),
                arguments(req4, res4)
        );
    }

    private static void assert_response(ExportResponse actual, ExportResponse expected) {
        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(actual.getKickOffRequestUrl()).isEqualTo(expected.getKickOffRequestUrl());
        softly.assertThat(actual.getWriteDetails()).isNotNull();
        softly.assertThat(actual.getWriteDetails().fileInfos())
                .isNotNull()
                .containsAnyElementsOf(expected.getWriteDetails().fileInfos());
        softly.assertAll();
    }

    private static ExportResponse resolveTempDirIn(ExportResponse exportResponse, Path tempDir) {
        List<DataSink.FileInfo> newFileInfos = exportResponse.getWriteDetails().fileInfos().stream()
                .map(
                        fileInfo -> fi(fileInfo.fhirResourceType(),
                                fileInfo.absoluteUrl().replace("WAREHOUSE_PATH", "file://" + tempDir.toAbsolutePath().toString()),
                                fileInfo.count()))
                .toList();
        return new ExportResponse(
                exportResponse.getKickOffRequestUrl(),
                write_details(newFileInfos)
        );
    }
}
