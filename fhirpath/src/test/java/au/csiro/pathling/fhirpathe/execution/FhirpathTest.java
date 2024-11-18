package au.csiro.pathling.fhirpathe.execution;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.execution.CollectionDataset;
import au.csiro.pathling.fhirpath.execution.FhirPathExecutor;
import au.csiro.pathling.fhirpath.execution.SingleFhirPathExecutor;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.assertions.DatasetAssert;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import ca.uhn.fhir.context.FhirContext;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Reference;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import scala.collection.mutable.WrappedArray;

/**
 * This is a test class to explore issues related to implementation of reverseResolve and resolve functions.
 */
@SpringBootUnitTest
@Slf4j
class FhirpathTest {

  @Autowired
  SparkSession spark;
  
  @Autowired
  FhirEncoders encoders;

  
  final Parser parser = new Parser();

  @Test
  void singleResourceTest() {
    final Patient patient = new Patient();
    patient.setId("1");
    patient.setGender(AdministrativeGender.FEMALE);
    patient.addName().setFamily("Kay").addGiven("Awee");
    patient.addName().setFamily("Kay").addGiven("Zosia");
    final ObjectDataSource dataSource = new ObjectDataSource(spark, encoders,
        List.of(patient));

    final Dataset<Row> result = execFhirPath(
        "where(gender='female').name.where(family.where($this='Kay').exists()).given.join(',')",
        dataSource);
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
  void simpleReverseResolveToSingularValue() {
    //final Dataset<Row> joinedResult = evalFhirPathMulti("reverseResolve(Condition.subject).id", dataSource);
    //joinedResult.show();
    final ObjectDataSource dataSource = new ObjectDataSource(spark, encoders,
        List.of(
            new Patient().setGender(AdministrativeGender.FEMALE).setId("Patient/1"),
            new Patient().setGender(AdministrativeGender.MALE).setId("Patient/2"),
            new Condition().setSubject(new Reference("Patient/1")).setId("Condition/x"),
            new Condition().setSubject(new Reference("Patient/1")).setId("Condition/y")
        ));

    final CollectionDataset patientResult = evalFhirPath(ResourceType.PATIENT, "id", dataSource);
    // ignore value here - we could just use the resource dataset directly
    // TODO: get access  to it
    final Dataset<Row> patientDataset = patientResult.getDataset().select("id", "key");
    patientDataset.show();
    new DatasetAssert(patientDataset)
        .hasRowsUnordered(
            RowFactory.create("1", "Patient/1"),
            RowFactory.create("2", "Patient/2")
        );

    final CollectionDataset conditionResult = evalFhirPath(ResourceType.CONDITION, "id",
        dataSource);

    final Dataset<Row> conditionDataset = conditionResult.getDataset().select(
        functions.col("Condition.subject.reference").alias("_master_key"),
        conditionResult.getValue().getColumnValue().alias("value"));
    conditionDataset.show();
    new DatasetAssert(conditionDataset)
        .hasRowsUnordered(
            RowFactory.create("Patient/1", "x"),
            RowFactory.create("Patient/1", "y")
        );

    final Dataset<Row> foreignResult = conditionDataset.groupBy(
            functions.col("_master_key"))
        .agg(functions.collect_list(functions.col("value")).alias("_fv_01"));
    foreignResult.show();
    
    final Dataset<Row> joinedResult = patientDataset.join(foreignResult,
            patientDataset.col("key").equalTo(foreignResult.col("_master_key")), "left_outer")
        .select(functions.col("id"), functions.col("_fv_01").alias("value"));
    joinedResult.show();
    System.out.println(joinedResult.queryExecution().executedPlan().toString());
    new DatasetAssert(joinedResult)
        .hasRowsUnordered(
            RowFactory.create("1", WrappedArray.make(new String[]{"x", "y"})),
            RowFactory.create("2", null)
        );
  }


  @Test
  void simpleReverseResolveToManyValue() {
    //final Dataset<Row> joinedResult = evalFhirPathMulti("reverseResolve(Condition.subject).code.coding.code", dataSource);
    //joinedResult.show();
    final ObjectDataSource dataSource = new ObjectDataSource(spark, encoders,
        List.of(
            new Patient().setGender(AdministrativeGender.FEMALE).setId("Patient/1"),
            new Patient().setGender(AdministrativeGender.MALE).setId("Patient/2"),
            new Condition()
                .setSubject(new Reference("Patient/1"))
                .setCode(
                    new CodeableConcept()
                        .addCoding(new Coding().setCode("code-xx"))
                        .addCoding(new Coding().setCode("code-xy"))
                        .setText("Coding-x")
                )
                .setId("Condition/x"),
            new Condition()
                .setSubject(new Reference("Patient/1"))
                .setCode(
                    new CodeableConcept()
                        .addCoding(new Coding().setCode("code-yx"))
                        .addCoding(new Coding().setCode("code-yy"))
                        .setText("Coding-y")
                )
                .setId("Condition/y")
        ));

    final CollectionDataset patientResult = evalFhirPath(ResourceType.PATIENT, "id", dataSource);
    // ignore value here - we could just use the resource dataset directly
    // TODO: get access  to it
    final Dataset<Row> patientDataset = patientResult.getDataset().select("id", "key");
    patientDataset.show();
    new DatasetAssert(patientDataset)
        .hasRowsUnordered(
            RowFactory.create("1", "Patient/1"),
            RowFactory.create("2", "Patient/2")
        );

    final CollectionDataset conditionResult = evalFhirPath(ResourceType.CONDITION,
        "code.coding.code",
        dataSource);

    final Dataset<Row> conditionDataset = conditionResult.getDataset().select(
        functions.col("Condition.subject.reference").alias("_master_key"),
        conditionResult.getValue().getColumnValue().alias("value"));

    conditionDataset.show();
    new DatasetAssert(conditionDataset)
        .hasRowsUnordered(
            RowFactory.create("Patient/1", sql_array("code-xx", "code-xy")),
            RowFactory.create("Patient/1", sql_array("code-yx", "code-yy"))
        );

    final Dataset<Row> foreignResult = conditionDataset.groupBy(
            functions.col("_master_key"))
        .agg(functions.flatten(functions.collect_list(functions.col("value"))).alias("_fv_01"));
    foreignResult.show();
    
    final Dataset<Row> joinedResult = patientDataset.join(foreignResult,
            patientDataset.col("key").equalTo(foreignResult.col("_master_key")), "left_outer")
        .select(functions.col("id"), functions.col("_fv_01").alias("value"));
    joinedResult.show();
    System.out.println(joinedResult.queryExecution().executedPlan().toString());
    new DatasetAssert(joinedResult)
        .hasRowsUnordered(
            RowFactory.create("1", sql_array("code-xx", "code-xy", "code-yx", "code-yy")),
            RowFactory.create("2", null)
        );
  }


  @Test
  void simpleReverseResolveToLeafAggregateFunction() {
    //final Dataset<Row> joinedResult = evalFhirPathMulti("reverseResolve(Condition.subject).code.coding.code.count()", dataSource);
    //joinedResult.show();
    final ObjectDataSource dataSource = new ObjectDataSource(spark, encoders,
        List.of(
            new Patient().setGender(AdministrativeGender.FEMALE).setId("Patient/1"),
            new Patient().setGender(AdministrativeGender.MALE).setId("Patient/2"),
            new Condition()
                .setSubject(new Reference("Patient/1"))
                .setCode(
                    new CodeableConcept()
                        .addCoding(new Coding().setCode("code-xx"))
                        .addCoding(new Coding().setCode("code-xy"))
                        .setText("Coding-x")
                )
                .setId("Condition/x"),
            new Condition()
                .setSubject(new Reference("Patient/1"))
                .setCode(
                    new CodeableConcept()
                        .addCoding(new Coding().setCode("code-yx"))
                        .addCoding(new Coding().setCode("code-yy"))
                        .setText("Coding-y")
                )
                .setId("Condition/y")
        ));

    final CollectionDataset patientResult = evalFhirPath(ResourceType.PATIENT, "id", dataSource);
    // ignore value here - we could just use the resource dataset directly
    // TODO: get access  to it
    final Dataset<Row> patientDataset = patientResult.getDataset().select("id", "key");
    patientDataset.show();
    new DatasetAssert(patientDataset)
        .hasRowsUnordered(
            RowFactory.create("1", "Patient/1"),
            RowFactory.create("2", "Patient/2")
        );

    final CollectionDataset conditionResult = evalFhirPath(ResourceType.CONDITION,
        "code.coding.code.count()",
        dataSource);

    final Dataset<Row> conditionDataset = conditionResult.getDataset().select(
        functions.col("Condition.subject.reference").alias("_master_key"),
        conditionResult.getValue().getColumnValue().alias("value"));

    conditionDataset.show();
    new DatasetAssert(conditionDataset)
        .hasRowsUnordered(
            RowFactory.create("Patient/1", 2),
            RowFactory.create("Patient/1", 2)
        );

    final Dataset<Row> foreignResult = conditionDataset.groupBy(
            functions.col("_master_key"))
        .agg(functions.sum(functions.col("value")).alias("_fv_01"));
    foreignResult.show();
    
    final Dataset<Row> joinedResult = patientDataset.join(foreignResult,
            patientDataset.col("key").equalTo(foreignResult.col("_master_key")), "left_outer")
        .select(functions.col("id"), functions.col("_fv_01").alias("value"));
    joinedResult.show();
    System.out.println(joinedResult.queryExecution().executedPlan().toString());
    new DatasetAssert(joinedResult)
        .hasRowsUnordered(
            RowFactory.create("1", 4),
            RowFactory.create("2", null)
        );
  }


  @Nonnull
  private static <T> WrappedArray<T> sql_array(final T... values) {
    return WrappedArray.make(values);
  }

  @Nonnull
  private Dataset<Row> execFhirPath(final ResourceType subjectResourceType,
      final String fhirpathExpression,
      final ObjectDataSource dataSource) {
    final FhirPath path = parser.parse(fhirpathExpression);
    System.out.println(path.toExpression());
    final FhirPathExecutor executor = new SingleFhirPathExecutor(subjectResourceType,
        FhirContext.forR4(), StaticFunctionRegistry.getInstance(),
        Collections.emptyMap(), dataSource);

    return executor.execute(path);
  }

  @Nonnull
  private CollectionDataset evalFhirPath(final ResourceType subjectResourceType,
      final String fhirpathExpression,
      final ObjectDataSource dataSource) {
    final FhirPath path = parser.parse(fhirpathExpression);
    System.out.println(path.toExpression());
    final FhirPathExecutor executor = new SingleFhirPathExecutor(subjectResourceType,
        FhirContext.forR4(), StaticFunctionRegistry.getInstance(),
        Collections.emptyMap(), dataSource);

    return executor.evaluate(path);
  }

  @Nonnull
  private Dataset<Row> execFhirPath(final String fhirpathExpression,
      final ObjectDataSource dataSource) {
    return execFhirPath(ResourceType.PATIENT, fhirpathExpression, dataSource);
  }
}
