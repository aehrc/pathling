/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.execution;

import static au.csiro.pathling.test.helpers.SqlHelpers.sql_array;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.execution.FhirPathEvaluators.SingleEvaluatorProvider;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.assertions.Assertions;
import au.csiro.pathling.test.assertions.DatasetAssert;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Coverage;
import org.hl7.fhir.r4.model.Device;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.HumanName;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * This is a test class to explore issues related to implementation of reverseResolve and resolve
 * functions.
 * <p>
 * This attempts to use 'purification approach' where elements that are not pure are replaced with
 * pure elements in a preprocessing step that constructs the input dataset.
 */
@SpringBootUnitTest
@Slf4j
class SingleResourceFhirpathTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirEncoders encoders;

  @Autowired
  TerminologyService terminologyService;

  @Nonnull
  CollectionDataset evalExpression(@Nonnull final ObjectDataSource dataSource,
      @Nonnull final ResourceType subjectResource,
      @Nonnull final String fhirExpression) {

    return createEvaluator(dataSource)
        .evaluate(subjectResource, fhirExpression)
        .toCanonical();

  }

  @Nonnull
  Dataset<Row> selectExpression(@Nonnull final ObjectDataSource dataSource,
      @Nonnull final ResourceType subjectResource,
      @Nonnull final String fhirExpression) {
    return evalExpression(dataSource, subjectResource, fhirExpression)
        .toCanonical()
        .toIdValueDataset();
  }

  @Nonnull
  FhirpathExecutor createEvaluator(@Nonnull final DataSource datasource) {
    return FhirpathExecutor.of(new Parser(), new SingleEvaluatorProvider(encoders.getContext(),
        StaticFunctionRegistry.getInstance(),
        Map.of(),
        datasource));
  }

  @Test
  void singleResourceTest() {
    final Patient patient = new Patient();
    patient.setId("1");
    patient.setGender(AdministrativeGender.FEMALE);
    patient.addName().setFamily("Kay").addGiven("Awee");
    patient.addName().setFamily("Kay").addGiven("Zosia");
    final ObjectDataSource dataSource = new ObjectDataSource(spark, encoders,
        List.of(patient));

    final Dataset<Row> result = selectExpression(dataSource, ResourceType.PATIENT,
        "where(gender='female').name.where(family.where($this='Kay').exists()).given.join(',')");
    final Dataset<Row> expected = DatasetBuilder.of(spark)
        .withColumn("id", DataTypes.StringType)
        .withColumn("value", DataTypes.StringType)
        .withRow("1", "Awee,Zosia")
        .build();

    new DatasetAssert(result)
        .hasRowsUnordered(expected);
  }


  @Test
  void nullHandlingTests() {

    final ObjectDataSource dataSource = new ObjectDataSource(spark, encoders,
        List.of(
            new Patient()
                .addName(new HumanName().setFamily("Kay").addGiven("Adam").addGiven("John"))
                .addName(new HumanName().setFamily("Kay").addGiven("Peter"))
                .addName(new HumanName().setFamily("Kay"))
                .setId("1"),
            new Patient()
                .addName(new HumanName().setFamily("Kay").addGiven("Awee"))
                .setId("2"),
            new Patient().setId("3")
        ));

    final Dataset<Row> result = selectExpression(dataSource, ResourceType.PATIENT,
        "name.given");
    new DatasetAssert(result)
        .hasRowsUnordered(
            RowFactory.create("1", sql_array("Adam", "John", "Peter")),
            RowFactory.create("2", sql_array("Awee")),
            RowFactory.create("3", null)
        );
  }

  @Test
  void resourceExtensionTest() {
    final ObjectDataSource dataSource = getExtensionTestSource();

    final Dataset<Row> resultDataset = selectExpression(dataSource, ResourceType.PATIENT,
        "extension('urn:ex1').value.ofType(string)");
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", sql_array("value1.1.1", "value1.1.2")),
            RowFactory.create("2", null),
            RowFactory.create("3", null)
        );
  }

  @Test
  void nestedExtensionTest() {
    final ObjectDataSource dataSource = getExtensionTestSource();

    final Dataset<Row> resultDataset = selectExpression(dataSource, ResourceType.PATIENT,
        "extension('urn:ex3').extension('urn:ex3_1').value.ofType(string)");
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", sql_array("value1.3_1.1")),
            RowFactory.create("2", null),
            RowFactory.create("3", null)
        );
  }


  @Test
  void nestedExtensionTraversalTest() {
    final ObjectDataSource dataSource = getExtensionTestSource();
    final Dataset<Row> resultDataset = selectExpression(dataSource, ResourceType.PATIENT,
        "extension.extension.url");
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", sql_array("urn:ex3_1")),
            RowFactory.create("2", null),
            RowFactory.create("3", null)
        );
  }

  @Test
  void elementExtensionTest() {
    final ObjectDataSource dataSource = getExtensionTestSource();

    final Dataset<Row> resultDataset = selectExpression(dataSource, ResourceType.PATIENT,
        "name.extension('urn:name1').value.ofType(string)");
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", null),
            RowFactory.create("2", sql_array("value1")),
            RowFactory.create("3", null)
        );
  }

  @Test
  void ofTypeExtensionTest() {
    final ObjectDataSource dataSource = getExtensionTestSource();

    final Dataset<Row> resultDataset = selectExpression(dataSource, ResourceType.PATIENT,
        "extension('urn:ex2').value.ofType(integer)");
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", null),
            RowFactory.create("2", sql_array(13)),
            RowFactory.create("3", null)
        );
  }

  @Nonnull
  private ObjectDataSource getExtensionTestSource() {
    return new ObjectDataSource(spark, encoders,
        List.of(
            new Patient()
                .addExtension(new Extension("urn:ex1", new StringType("value1.1.1")))
                .addExtension(new Extension("urn:ex1", new StringType("value1.1.2")))
                .addExtension(new Extension("urn:ex2", new StringType("value1.2.1")))
                .addExtension((Extension) new Extension("urn:ex3").addExtension(
                    new Extension("urn:ex3_1", new StringType("value1.3_1.1")))
                )
                .setId("Patient/1"),
            new Patient()
                .addName((HumanName) new HumanName().setFamily("Kay").addGiven("Awee")
                    .addExtension(new Extension("urn:name1", new StringType("value1"))))
                .addExtension(new Extension("urn:ex2", new StringType("value1.2.1")))
                .addExtension(new Extension("urn:ex2", new IntegerType(13)))
                .setId("Patient/2"),
            new Patient()
                .setId("Patient/3")
        )
    );
  }

  @Test
  void testOfTypeForChoice() {
    final ObjectDataSource dataSource = new ObjectDataSource(spark, encoders,
        List.of(
            new Observation()
                .setValue(new IntegerType("17"))
                .setId("Observation/1"),
            new Observation()
                .setValue(new StringType("value1"))
                .setId("Observation/2"),
            new Observation()
                .setId("Observation/3")
        )
    );

    final CollectionDataset evalResult = evalExpression(dataSource, ResourceType.OBSERVATION,
        "value.ofType(integer)");

    Assertions.assertThat(evalResult)
        .hasClass(IntegerCollection.class)
        .toCanonicalResult()
        .hasRowsUnordered(
            RowFactory.create("1", 17),
            RowFactory.create("2", null),
            RowFactory.create("3", null)
        );
  }

  @Test
  void testFHIRTypeOfIntegerMathOperation() {
    final ObjectDataSource dataSource = new ObjectDataSource(spark, encoders,
        List.of(
            new Coverage()
                .setOrder(1)
                .setId("Coverage/1")
        )
    );

    final CollectionDataset evalResult = evalExpression(dataSource, ResourceType.COVERAGE,
        "order - 11");

    Assertions.assertThat(evalResult)
        .hasClass(IntegerCollection.class)
        .hasFhirType(FHIRDefinedType.INTEGER)
        .toCanonicalResult()
        .hasRowsUnordered(
            RowFactory.create("1", -10)
        );
  }


  @Test
  void testOfTypeForReference() {
    final ObjectDataSource dataSource = new ObjectDataSource(spark, encoders,
        List.of(
            new Observation()
                .addExtension(new Extension("urn:ref", new Reference("MolecularSequence/1")))
                .setId("Observation/1"),
            new Observation()
                .setId("Observation/2")
        )
    );

    final CollectionDataset evalResult = evalExpression(dataSource, ResourceType.OBSERVATION,
        "extension.value.ofType(Reference).reference");

    Assertions.assertThat(evalResult)
        .hasClass(StringCollection.class)
        .toCanonicalResult()
        .hasRowsUnordered(
            RowFactory.create("1", sql_array("MolecularSequence/1")),
            RowFactory.create("2", null)
        );
  }


  @Test
  void testContainsWithCodingLiteral() {

    final ObjectDataSource dataSource = new ObjectDataSource(spark, encoders,
        List.of(
            new Patient()
                .setMaritalStatus(new CodeableConcept()
                    .addCoding(new Coding()
                        .setSystem("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus")
                        .setCode("S")
                        .setDisplay("S")))
                .setId("Patient/1")
        )
    );

    final CollectionDataset evalResult = evalExpression(dataSource, ResourceType.PATIENT,
        "maritalStatus.coding contains http://terminology.hl7.org/CodeSystem/v3-MaritalStatus|S||S");

    Assertions.assertThat(evalResult)
        .hasClass(BooleanCollection.class)
        .toCanonicalResult()
        .hasRowsUnordered(
            RowFactory.create("1", true)
        );
  }


  @Test
  void testComparisonBase64Binary() {

    final ObjectDataSource dataSource = new ObjectDataSource(spark, encoders,
        List.of(
            new Device()
                .addUdiCarrier(new Device.DeviceUdiCarrierComponent()
                    .setCarrierAIDC("AID1".getBytes())
                    .setCarrierHRF("HRF1"))
                .setId("Device/1")
        )
    );

    final CollectionDataset evalResult = evalExpression(dataSource, ResourceType.DEVICE,
        "udiCarrier.carrierAIDC = 'QUlEMQ=='");

    Assertions.assertThat(evalResult)
        .hasClass(BooleanCollection.class)
        .toCanonicalResult()
        .explain()
        .hasRowsUnordered(
            RowFactory.create("1", true)
        );
  }

  @Test
  void testSubsumesSingularCodeableConcept() {

    TerminologyServiceHelpers.setupSubsumes(terminologyService);
    final ObjectDataSource dataSource = codedObservations();

    final CollectionDataset evalResult = evalExpression(dataSource, ResourceType.OBSERVATION,
        "code.subsumes(http://loinc.org|LA11165-0||Platelet%20anisocytosis)");

    Assertions.assertThat(evalResult)
        .hasClass(BooleanCollection.class)
        .toCanonicalResult()
        .hasRowsUnordered(
            RowFactory.create("1", true)
        );
  }

  @Test
  void testSubsumesPluralCodeableConcept() {

    TerminologyServiceHelpers.setupSubsumes(terminologyService);
    final ObjectDataSource dataSource = codedObservations();

    final CollectionDataset evalResult = evalExpression(dataSource, ResourceType.OBSERVATION,
        "category.subsumes(http://terminology.hl7.org/CodeSystem/v3-ObservationCategory|vital-signs||Vital%20Signs)");

    Assertions.assertThat(evalResult)
        .hasClass(BooleanCollection.class)
        .toCanonicalResult()
        .hasRowsUnordered(
            RowFactory.create("1", sql_array(true, false))
        );
  }


  @Test
  void testSubsumesPluralCoding() {

    TerminologyServiceHelpers.setupSubsumes(terminologyService);
    final ObjectDataSource dataSource = codedObservations();

    final CollectionDataset evalResult = evalExpression(dataSource, ResourceType.OBSERVATION,
        "code.coding.subsumes(http://loinc.org|LA11165-0||Platelet%20anisocytosis)");

    Assertions.assertThat(evalResult)
        .hasClass(BooleanCollection.class)
        .toCanonicalResult()
        .hasRowsUnordered(
            RowFactory.create("1", sql_array(true, false))
        );
  }


  @Test
  void testSubsumesSingularCoding() {

    TerminologyServiceHelpers.setupSubsumes(terminologyService);
    final ObjectDataSource dataSource = codedObservations();

    final CollectionDataset evalResult = evalExpression(dataSource, ResourceType.OBSERVATION,
        "code.coding.first().subsumes(http://loinc.org|LA11165-0||Platelet%20anisocytosis)");

    Assertions.assertThat(evalResult)
        .hasClass(BooleanCollection.class)
        .toCanonicalResult()
        .hasRowsUnordered(
            RowFactory.create("1", true)
        );
  }


  @Nonnull
  private ObjectDataSource codedObservations() {
    return new ObjectDataSource(spark, encoders,
        List.of(
            new Observation()
                .setCode(new CodeableConcept()
                    .addCoding(new Coding()
                        .setSystem("http://loinc.org")
                        .setCode("LA11165-0")
                        .setDisplay("Platelet anisocytosis"))
                    .addCoding(new Coding()
                        .setSystem("uuid:1")
                        .setCode("M")
                        .setDisplay("M"))
                )
                .addCategory(new CodeableConcept()
                    .addCoding(new Coding()
                        .setSystem("http://terminology.hl7.org/CodeSystem/v3-ObservationCategory")
                        .setCode("vital-signs")
                        .setDisplay("Vital Signs")
                    )
                    .addCoding(new Coding()
                        .setSystem("uuid:2")
                        .setCode("foo")
                        .setDisplay("Foo")
                    )
                )
                .addCategory(
                    new CodeableConcept()
                        .addCoding(new Coding()
                            .setSystem(
                                "http://terminology.hl7.org/CodeSystem/v3-ObservationCategory")
                            .setCode("laboratory")
                            .setDisplay("Laboratory")
                        )
                        .addCoding(new Coding()
                            .setSystem("uuid:2")
                            .setCode("bar")
                            .setDisplay("Bar")
                        )
                )
                .setId("Observation/1")
        )
    );
  }

}
