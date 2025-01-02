package au.csiro.pathling.fhirpathe.execution;

import static au.csiro.pathling.test.helpers.SqlHelpers.sql_array;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.ReferenceCollection;
import au.csiro.pathling.fhirpath.execution.CollectionDataset;
import au.csiro.pathling.fhirpath.execution.FhirPathEvaluator;
import au.csiro.pathling.fhirpath.execution.MultiFhirPathEvaluator;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.assertions.Assertions;
import au.csiro.pathling.test.assertions.DatasetAssert;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import jakarta.annotation.Nonnull;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.HumanName;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.Disabled;
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
class SingleResourceFhirpathTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirEncoders encoders;


  @Nonnull
  CollectionDataset evalExpression(@Nonnull final ObjectDataSource dataSource,
      @Nonnull final ResourceType subjectResource,
      @Nonnull final String fhirExpression) {

    return createEvaluator(subjectResource, dataSource)
        .evaluate(fhirExpression);

  }

  @Nonnull
  Dataset<Row> selectExpression(@Nonnull final ObjectDataSource dataSource,
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
    result.show();
    System.out.println(result.queryExecution().executedPlan().toString());
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
                .addName(new HumanName().setFamily("Kay").addGiven("Awee").setText("Awee Kay"))
                .addName(new HumanName().setFamily("Kay").addGiven("Awee"))
                .setId("1"),
            new Patient()
                .addName(new HumanName().setFamily("Kay").addGiven("Awee"))
                .setId("2"),
            new Patient().setId("3")
        ));

    final Dataset<Row> result = selectExpression(dataSource, ResourceType.PATIENT,
        "name.text");
    result.show();
    System.out.println(result.queryExecution().executedPlan().toString());

    new DatasetAssert(result)
        .hasRowsUnordered(
            RowFactory.create("1", sql_array("Awee Kay")),
            RowFactory.create("2", null),
            RowFactory.create("3", null)
        );
  }

  @Test
  void resourceExtensionTest() {
    final ObjectDataSource dataSource = getExtensionTestSource();

    final Dataset<Row> resultDataset = selectExpression(dataSource, ResourceType.PATIENT,
        "extension('urn:ex1').value.ofType(string)");
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    resultDataset.show();
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
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    resultDataset.show();
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", sql_array("value1.3_1.1")),
            RowFactory.create("2", null),
            RowFactory.create("3", null)
        );
  }


  @Disabled("Seems to be an issue with nested ifArray????")
  @Test
  void nestedExtensionTraversalTest() {
    final ObjectDataSource dataSource = getExtensionTestSource();

    // TODO: works ok with extension('urn:ex3').extension.url
    final Dataset<Row> resultDataset = selectExpression(dataSource, ResourceType.PATIENT,
        "extension.extension.url");
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    resultDataset.show();
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
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    resultDataset.show();
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
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    resultDataset.show();
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
        .isElementPath(IntegerCollection.class)
        .selectResult()
        .hasRowsUnordered(
            RowFactory.create("1", 17),
            RowFactory.create("2", null),
            RowFactory.create("3", null)
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
        "extension.value.ofType(Reference)");
    
    Assertions.assertThat(evalResult)
        .isElementPath(ReferenceCollection.class)
        .selectResult()
        .hasRowsUnordered(
            RowFactory.create("1", 17),
            RowFactory.create("2", null),
            RowFactory.create("3", null)
        );
  }

}
