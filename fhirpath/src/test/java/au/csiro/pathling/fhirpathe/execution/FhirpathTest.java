package au.csiro.pathling.fhirpathe.execution;

import static au.csiro.pathling.test.helpers.SqlHelpers.sql_array;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.execution.FhirPathEvaluator;
import au.csiro.pathling.fhirpath.execution.MultiFhirPathEvaluator;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.assertions.DatasetAssert;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import jakarta.annotation.Nonnull;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Appointment;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.DiagnosticReport;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Encounter.EncounterStatus;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.EpisodeOfCare;
import org.hl7.fhir.r4.model.EpisodeOfCare.EpisodeOfCareStatus;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.Goal;
import org.hl7.fhir.r4.model.Location;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Reference;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * This is a test class to explore issues related to implementation of reverseResolve and resolve
 * functions.
 * <p>
 * This attemps to use 'purification approch' where elements that are not pure are replaced with
 * pure elements in a preprocessing step that constructs the input dataset.
 */
@SpringBootUnitTest
@Slf4j
class FhirpathTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirEncoders encoders;

  @Autowired
  TerminologyService terminologyService;


  @Nonnull
  Dataset<Row> evalExpression(@Nonnull final ObjectDataSource dataSource,
      @Nonnull final ResourceType subjectResource,
      @Nonnull final String fhirExpression) {

    return createEvaluator(subjectResource, dataSource)
        .evaluate(fhirExpression)
        .toIdValueDataset();
  }

  @Nonnull
  FhirPathEvaluator createEvaluator(@Nonnull final ResourceType subjectResource,
      @Nonnull final DataSource datasource) {
    return new MultiFhirPathEvaluator(subjectResource, encoders.getContext(),
        StaticFunctionRegistry.getInstance(), datasource);
  }

  @Test
  void accessParentResourceInJoinedExpression() {
    TerminologyServiceHelpers.setupSubsumes(terminologyService);
    final ObjectDataSource dataSource = getPatientsWithConditions();
    final Dataset<Row> resultDataset = evalExpression(dataSource, ResourceType.PATIENT,
        "reverseResolve(Condition.subject).code.coding.subsumes(%resource.reverseResolve(Condition.subject).code)");
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    resultDataset.show();
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", true),
            RowFactory.create("2", true),
            RowFactory.create("3", null)
        );
  }

  @Test
  void simpleReverseResolveToSingularValue() {
    final ObjectDataSource dataSource = getPatientsWithConditions();
    final Dataset<Row> resultDataset = evalExpression(dataSource, ResourceType.PATIENT,
        "reverseResolve(Condition.subject).id");
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    resultDataset.show();
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", sql_array("x", "y")),
            RowFactory.create("2", sql_array("z")),
            RowFactory.create("3", null)
        );
  }

  @Test
  void simpleReverseResolveToManyValue() {
    final ObjectDataSource dataSource = getPatientsWithConditions();
    final Dataset<Row> resultDataset = evalExpression(dataSource, ResourceType.PATIENT,
        "reverseResolve(Condition.subject).code.coding.code");
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", sql_array("code-xx", "code-xy", "code-yx", "code-yy")),
            RowFactory.create("2", sql_array("code-zx", "code-zy", "code-zz")),
            RowFactory.create("3", null)
        );
  }

  @Test
  void simpleReverseResolveToLeafCountFunction() {
    final ObjectDataSource dataSource = getPatientsWithConditions();
    final Dataset<Row> resultDataset = evalExpression(dataSource, ResourceType.PATIENT,
        "reverseResolve(Condition.subject).code.coding.code.count()");

    // TODO: should be 0 in the last row

    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", 4),
            RowFactory.create("2", 3),
            RowFactory.create("3", 0)
        );
  }

  @Test
  void simpleReverseResolveToChildResourceCountFunction() {
    final ObjectDataSource dataSource = getPatientsWithConditions();
    final Dataset<Row> resultDataset = evalExpression(dataSource, ResourceType.PATIENT,
        "reverseResolve(Condition.subject).count()");

    // TODO: should be 0 in the last row

    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", 2),
            RowFactory.create("2", 1),
            RowFactory.create("3", 0)
        );
  }

  @Test
  void reverseResolveInWhereWithChildResourceCountFunction() {
    final ObjectDataSource dataSource = getPatientsWithConditions();
    final Dataset<Row> resultDataset = evalExpression(dataSource, ResourceType.PATIENT,
        "where(reverseResolve(Condition.subject).count() = 2).id");

    // TODO: should be 0 in the last row

    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", "1"),
            RowFactory.create("2", null),
            // TODO: count() should be 0
            RowFactory.create("3", null)
        );
  }

  @Test
  void simpleReverseResolveToLeafFirstFunction() {
    final ObjectDataSource dataSource = getPatientsWithConditions();
    final Dataset<Row> resultDataset = evalExpression(dataSource, ResourceType.PATIENT,
        "reverseResolve(Condition.subject).code.coding.code.first()");

    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", "code-xx"),
            RowFactory.create("2", "code-zx"),
            RowFactory.create("3", null)
        );
  }

  @Test
  void simpleReverseResolveToCountFirstFunction() {
    // TODO: Implement
    final ObjectDataSource dataSource = getPatientsWithConditions();
    final Dataset<Row> resultDataset = evalExpression(dataSource, ResourceType.PATIENT,
        "reverseResolve(Condition.subject).code.coding.code.count().first()");

    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", 4),
            RowFactory.create("2", 3),
            RowFactory.create("3", 0)
        );
  }

  @Test
  void simpleReverseResolveToFirstCountFunction() {
    // TODO: Implement
    final ObjectDataSource dataSource = getPatientsWithConditions();
    final Dataset<Row> resultDataset = evalExpression(dataSource, ResourceType.PATIENT,
        "reverseResolve(Condition.subject).code.coding.code.first().count()");

    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", 1),
            RowFactory.create("2", 1),
            RowFactory.create("3", 0)
        );
  }

  @Test
  void whereReverseResolveToSingularValue() {
    final ObjectDataSource dataSource = getPatientsWithConditions();
    final Dataset<Row> resultDataset = evalExpression(dataSource,
        ResourceType.PATIENT,
        "where(gender='female').reverseResolve(Condition.subject).id"
    );
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    resultDataset.show();
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", sql_array("x", "y")),
            RowFactory.create("2", null),
            RowFactory.create("3", null)
        );
  }

  @Test
  void multipleReverseResolveInOperator() {
    // TODO: Implement
    final ObjectDataSource dataSource = getPatientsWithConditions();
    final Dataset<Row> resultDataset = evalExpression(dataSource, ResourceType.PATIENT,
        "reverseResolve(Condition.subject).code.coding.count() + reverseResolve(Condition.subject).id.count()");

    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", 6),
            RowFactory.create("2", 4),
            RowFactory.create("3", 0)
        );
  }

  @Test
  void nestedReverseResolveToSingularValue() {
    final ObjectDataSource dataSource = getPatientsWithEncountersWithConditions();

    final Dataset<Row> resultDataset = evalExpression(dataSource,
        ResourceType.PATIENT,
        "reverseResolve(Encounter.subject).reverseResolve(Condition.encounter).id"
    );
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    resultDataset.show();
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", sql_array("1.1.1", "1.1.2", "1.1.3", "1.2.1")),
            RowFactory.create("2", sql_array("2.1.1", "2.1.2")),
            RowFactory.create("3", null)
        );
  }

  @Test
  void nestedReverseResolveToAggregation() {
    final ObjectDataSource dataSource = getPatientsWithEncountersWithConditions();

    final Dataset<Row> resultDataset = evalExpression(dataSource,
        ResourceType.PATIENT,
        "reverseResolve(Encounter.subject).reverseResolve(Condition.encounter).id.count()"
    );
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    resultDataset.show();

    // TODO: should be 0 in the last row

    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", 4),
            RowFactory.create("2", 2),
            RowFactory.create("3", 0)
        );
  }


  @Test
  void nestedResolveOneToValue() {
    final ObjectDataSource dataSource = getPatientsWithEncountersWithConditions();

    final Dataset<Row> resultDataset = evalExpression(dataSource,
        ResourceType.CONDITION,
        "encounter.resolve().subject.resolve().ofType(Patient).id"
    );
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    resultDataset.show();

    // TODO: should be 0 in the last row

    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1.1.1", "1"),
            RowFactory.create("1.1.2", "1"),
            RowFactory.create("1.1.3", "1"),
            RowFactory.create("1.2.1", "1"),
            RowFactory.create("2.1.1", "2"),
            RowFactory.create("2.1.2", "2")
        );
  }

  private @NotNull ObjectDataSource getPatientsWithEncountersWithConditions() {
    return new ObjectDataSource(spark, encoders,
        List.of(
            new Patient().setGender(AdministrativeGender.FEMALE).setId("Patient/1"),
            new Patient().setGender(AdministrativeGender.MALE).setId("Patient/2"),
            new Patient().setGender(AdministrativeGender.MALE).setId("Patient/3"),
            new Encounter().setSubject(new Reference("Patient/1")).setId("Encounter/1.1"),
            new Encounter().setSubject(new Reference("Patient/1")).setId("Encounter/1.2"),
            new Encounter().setSubject(new Reference("Patient/2")).setId("Encounter/2.1"),
            new Condition().setSubject(new Reference("Patient/1"))
                .setEncounter(new Reference("Encounter/1.1")).setId("Condition/1.1.1"),
            new Condition().setSubject(new Reference("Patient/1"))
                .setEncounter(new Reference("Encounter/1.1")).setId("Condition/1.1.2"),
            new Condition().setSubject(new Reference("Patient/1"))
                .setEncounter(new Reference("Encounter/1.1")).setId("Condition/1.1.3"),
            new Condition().setSubject(new Reference("Patient/1"))
                .setEncounter(new Reference("Encounter/1.2")).setId("Condition/1.2.1"),
            new Condition().setSubject(new Reference("Patient/2"))
                .setEncounter(new Reference("Encounter/2.1")).setId("Condition/2.1.1"),
            new Condition().setSubject(new Reference("Patient/2"))
                .setEncounter(new Reference("Encounter/2.1")).setId("Condition/2.1.2")
        ));
  }

  //
  // SECTION: Resolve
  //
  @Test
  void resolveManyToOneWithSimpleValue() {
    final ObjectDataSource dataSource = getPatientsWithConditions();

    final Dataset<Row> resultDataset = evalExpression(dataSource,
        ResourceType.CONDITION,
        "subject.resolve().ofType(Patient).gender"
    );
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    resultDataset.show();
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("x", "female"),
            RowFactory.create("y", "female"),
            RowFactory.create("z", "male")
        );
  }

  @Test
  void resolveToManyWithSimpleValue() {

    final ObjectDataSource dataSource = encoundersWithEpisodes();

    final Dataset<Row> resultDataset = evalExpression(dataSource,
        ResourceType.ENCOUNTER,
        "episodeOfCare.resolve().status"
    );
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    resultDataset.show();

    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("01", sql_array("active", "finished")),
            RowFactory.create("02", sql_array("onhold", "planned", "waitlist")),
            // TODO: How do we represent an empty array
            RowFactory.create("03", null)
        );
  }


  @Test
  void resolveToManyWithAggregation() {

    final ObjectDataSource dataSource = encoundersWithEpisodes();

    final Dataset<Row> resultDataset = evalExpression(dataSource,
        ResourceType.ENCOUNTER,
        "episodeOfCare.resolve().count()"
    );
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    resultDataset.show();

    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("01", 2),
            RowFactory.create("02", 3),
            RowFactory.create("03", 0)
        );
  }

  private @NotNull ObjectDataSource encoundersWithEpisodes() {
    return new ObjectDataSource(spark, encoders,
        List.of(
            new Encounter()
                .addEpisodeOfCare(new Reference("EpisodeOfCare/01_1"))
                .addEpisodeOfCare(new Reference("EpisodeOfCare/01_2"))
                .setId("Encounter/01"),
            new Encounter()
                .addEpisodeOfCare(new Reference("EpisodeOfCare/02_1"))
                .addEpisodeOfCare(new Reference("EpisodeOfCare/02_2"))
                .addEpisodeOfCare(new Reference("EpisodeOfCare/02_3"))
                .setId("Encounter/02"),
            new Encounter().setId("Encounter/03"),
            new EpisodeOfCare().setStatus(EpisodeOfCareStatus.ACTIVE).setId("EpisodeOfCare/01_1"),
            new EpisodeOfCare().setStatus(EpisodeOfCareStatus.FINISHED)
                .setId("EpisodeOfCare/01_2"),
            new EpisodeOfCare().setStatus(EpisodeOfCareStatus.ONHOLD).setId("EpisodeOfCare/02_1"),
            new EpisodeOfCare().setStatus(EpisodeOfCareStatus.PLANNED).setId("EpisodeOfCare/02_2"),
            new EpisodeOfCare().setStatus(EpisodeOfCareStatus.WAITLIST).setId("EpisodeOfCare/02_3")
        ));
  }

  @Test
  void resolveToManyWithSimpleValueAndWhereInPath() {

    final ObjectDataSource dataSource = new ObjectDataSource(spark, encoders,
        List.of(
            new Patient().setGender(AdministrativeGender.MALE).setId("Patient/1"),
            new Patient().setGender(AdministrativeGender.FEMALE).setId("Patient/2"),
            new Encounter()
                .setSubject(new Reference("Patient/1"))
                .setStatus(EncounterStatus.ARRIVED)
                .addEpisodeOfCare(new Reference("EpisodeOfCare/01_1"))
                .addEpisodeOfCare(new Reference("EpisodeOfCare/01_2"))
                .setId("Encounter/01"),
            new Encounter()
                .setSubject(new Reference("Patient/2"))
                .setStatus(EncounterStatus.CANCELLED)
                .addEpisodeOfCare(new Reference("EpisodeOfCare/02_1"))
                .addEpisodeOfCare(new Reference("EpisodeOfCare/02_2"))
                .setId("Encounter/02"),
            new Encounter().setId("Encounter/03"),
            new EpisodeOfCare().setStatus(EpisodeOfCareStatus.ACTIVE).setId("EpisodeOfCare/01_1"),
            new EpisodeOfCare().setStatus(EpisodeOfCareStatus.FINISHED)
                .setId("EpisodeOfCare/01_2"),
            new EpisodeOfCare().setStatus(EpisodeOfCareStatus.ONHOLD).setId("EpisodeOfCare/02_1"),
            new EpisodeOfCare().setStatus(EpisodeOfCareStatus.PLANNED).setId("EpisodeOfCare/02_2")
        ));

    final Dataset<Row> resultDataset = evalExpression(dataSource,
        ResourceType.ENCOUNTER,
        "where(subject.resolve().ofType(Patient).gender = 'male').episodeOfCare.resolve().status"
    );
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    resultDataset.show();

    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("01", sql_array("active", "finished")),
            RowFactory.create("02", null),
            // TODO: How do we represent an empty array
            RowFactory.create("03", null)
        );
  }


  @Nonnull
  private ObjectDataSource getPatientsWithConditions() {
    return new ObjectDataSource(spark, encoders,
        List.of(
            new Patient().setGender(AdministrativeGender.FEMALE).setId("Patient/1"),
            new Patient().setGender(AdministrativeGender.MALE).setId("Patient/2"),
            new Patient().setGender(AdministrativeGender.MALE).setId("Patient/3"),
            new Condition()
                .setSubject(new Reference("Patient/1"))
                .setCode(
                    new CodeableConcept()
                        .addCoding(new Coding().setCode("code-xx").setSystem("system-x"))
                        .addCoding(new Coding().setCode("code-xy").setSystem("system-x"))
                        .setText("Coding-x")
                )
                .setId("Condition/x"),
            new Condition()
                .setSubject(new Reference("Patient/1"))
                .setCode(
                    new CodeableConcept()
                        .addCoding(new Coding().setCode("code-yx").setSystem("system-x"))
                        .addCoding(new Coding().setCode("code-yy").setSystem("system-x"))
                        .setText("Coding-y")
                )
                .setId("Condition/y"),
            new Condition()
                .setSubject(new Reference("Patient/2"))
                .setCode(
                    new CodeableConcept()
                        .addCoding(new Coding().setCode("code-zx").setSystem("system-x"))
                        .addCoding(new Coding().setCode("code-zy").setSystem("system-x"))
                        .addCoding(new Coding().setCode("code-zz").setSystem("system-x"))
                        .setText("Coding-z")
                )
                .setId("Condition/z")
        ));
  }

  @Test
  void reverseResolveBackToResolved() {
    final ObjectDataSource dataSource = getPatientsWithConditions();

    final Dataset<Row> resultDataset = evalExpression(dataSource,
        ResourceType.CONDITION,
        "subject.resolve().ofType(Patient).reverseResolve(Condition.subject).id"
    );
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    resultDataset.show();
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("x", sql_array("x", "y")),
            RowFactory.create("y", sql_array("x", "y")),
            RowFactory.create("z", sql_array("z"))
        );
  }

  @Test
  void resolveBackFromReverseResolve() {
    final ObjectDataSource dataSource = getPatientsWithConditions();

    final Dataset<Row> resultDataset = evalExpression(dataSource,
        ResourceType.PATIENT,
        "reverseResolve(Condition.subject).subject.resolve().ofType(Patient).id"
    );
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    resultDataset.show();
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", sql_array("1", "1")),
            RowFactory.create("2", sql_array("2")),
            RowFactory.create("3", null)
        );
  }

  @Test
  void multipleResolveToOneToTheSameResourceOnDiffernetPaths() {
    final ObjectDataSource dataSource =
        new ObjectDataSource(spark, encoders,
            List.of(
                new Encounter()
                    .setHospitalization(new Encounter.EncounterHospitalizationComponent()
                        .setOrigin(new Reference("Location/1"))
                        .setDestination(new Reference("Location/2"))
                    )
                    .setId("Encounter/1"),
                new Encounter()
                    .setHospitalization(new Encounter.EncounterHospitalizationComponent()
                        .setOrigin(new Reference("Location/3"))
                        .setDestination(new Reference("Location/3"))
                    )
                    .setId("Encounter/2"),
                new Encounter()
                    .setHospitalization(new Encounter.EncounterHospitalizationComponent()
                        .setOrigin(new Reference("Location/3"))
                    )
                    .setId("Encounter/3"),
                new Location().setId("Location/1"),
                new Location().setId("Location/2"),
                new Location().setId("Location/3")
            ));

    final Dataset<Row> resultDataset = evalExpression(dataSource,
        ResourceType.ENCOUNTER,
        "hospitalization.origin.resolve().ofType(Location).count()"
            + "=hospitalization.destination.resolve().ofType(Location).count()"
    );
    System.out.println(resultDataset.queryExecution().executedPlan().toString());

    resultDataset.show();
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", true),
            RowFactory.create("2", true),
            RowFactory.create("3", false)
        );
  }

  @Test
  void multipleResolveToManyTheSameResourceInSubresolve() {
    final ObjectDataSource dataSource =
        new ObjectDataSource(spark, encoders,
            List.of(
                new Encounter()
                    .addAppointment(new Reference("Appointment/1.1"))
                    .addReasonReference(new Reference("Condition/1.1"))
                    .setId("Encounter/1"),
                new Appointment()
                    .addReasonReference(new Reference("Condition/1.1"))
                    .setId("Appointment/1.1"),
                new Condition().setId("Condition/1.1"),
                new Encounter()
                    .addAppointment(new Reference("Appointment/2.1"))
                    .addReasonReference(new Reference("Condition/2.1"))
                    .setId("Encounter/2"),
                new Appointment()
                    .addReasonReference(new Reference("Condition/2.2"))
                    .setId("Appointment/2.1"),
                new Condition().setId("Condition/2.1"),
                new Condition().setId("Condition/2.2")
            ));

    final Dataset<Row> resultDataset = evalExpression(dataSource,
        ResourceType.ENCOUNTER,
        "appointment.resolve().reasonReference.resolve().ofType(Condition).id.first()"
            + " = reasonReference.resolve().ofType(Condition).id.first()"
    );
    resultDataset.show();
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", true),
            RowFactory.create("2", false)
        );
  }


  @Test
  void chainedReferenceToSameResouece() {
    final ObjectDataSource dataSource =
        new ObjectDataSource(spark, encoders,
            List.of(
                new Observation()
                    .addHasMember(new Reference("Observation/2"))
                    .setId("Observation/1"),
                new Observation()
                    .addHasMember(new Reference("Observation/3"))
                    .setId("Observation/2"),
                new Observation()
                    .addHasMember(new Reference("Observation/4"))
                    .setId("Observation/3"),
                new Observation().setId("Observation/4")
            ));

    final Dataset<Row> resultDataset = evalExpression(dataSource,
        ResourceType.OBSERVATION,
        "hasMember.resolve().ofType(Observation)"
            + ".hasMember.resolve().ofType(Observation)"
            + ".hasMember.resolve().ofType(Observation)"
            + ".count()"
            + " + hasMember.resolve().ofType(Observation).count()"
            + " + hasMember.resolve().ofType(Observation).hasMember.resolve().ofType(Observation).count()"
    );
    resultDataset.show();
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", 3),
            RowFactory.create("2", 2),
            RowFactory.create("3", 1),
            RowFactory.create("4", 0)
        );
  }

  @Test
  void extensionReference() {
    final ObjectDataSource dataSource =
        new ObjectDataSource(spark, encoders,
            List.of(
                new Encounter()
                    .addExtension(new Extension("urn:goal", new Reference("Goal/1")))
                    .setId("Encounter/1"),
                new Goal().setId("Goal/1"),
                new Encounter().setId("Encounter/2")
            ));

    final Dataset<Row> resultDataset = evalExpression(dataSource,
        ResourceType.ENCOUNTER,
        "extension('urn:goal').value.ofType(Reference).resolve().ofType(Goal).id"
    );
    resultDataset.show();
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", 3),
            RowFactory.create("2", 2),
            RowFactory.create("3", 1),
            RowFactory.create("4", 0)
        );
  }

  @Test
  void testCombineOperatorWithTwoUntypedResourcePaths() {
    final ObjectDataSource dataSource =
        new ObjectDataSource(spark, encoders,
            List.of(
                new Patient().setId("Patient/1"),
                new Condition()
                    .setSubject(new Reference("Patient/1"))
                    .setId("Condition/1"),
                new DiagnosticReport()
                    .setSubject(new Reference("Patient/1"))
                    .setId("DiagnosticReport/1")
            ));

    final Dataset<Row> resultDataset = evalExpression(dataSource,
        ResourceType.PATIENT,
        "(reverseResolve(Condition.subject).subject.resolve() combine "
            + "reverseResolve(DiagnosticReport.subject).subject.resolve()).ofType(Patient).id"
    );
    resultDataset.show();
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", sql_array("1", "1"))
        );
  }


  @Test
  void testIfFunctionWithUntypedResourceResult() {

    final ObjectDataSource dataSource =
        new ObjectDataSource(spark, encoders,
            List.of(
                new Patient()
                    .setGender(AdministrativeGender.MALE)
                    .addLink(new Patient.PatientLinkComponent()
                        .setType(Patient.LinkType.REPLACEDBY)
                        .setOther(new Reference("Patient/2")))
                    .setId("Patient/1"),
                new Patient()
                    .setGender(AdministrativeGender.UNKNOWN)
                    .setId("Patient/2"),
                new Patient().setId("Patient/3")
            ));

    final Dataset<Row> resultDataset = evalExpression(dataSource,
        ResourceType.PATIENT,
        "iif(gender = 'male', link.where(type = 'replaced-by').other.resolve(), "
            + "link.where(type = 'replaces').other.resolve()).ofType(Patient).gender"
    );
    resultDataset.show();
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", "unknown"),
            RowFactory.create("2", null),
            RowFactory.create("3", null)
        );
  }

}
