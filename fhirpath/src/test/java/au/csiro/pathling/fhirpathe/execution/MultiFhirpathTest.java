package au.csiro.pathling.fhirpathe.execution;

import static au.csiro.pathling.test.helpers.SqlHelpers.sql_array;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.execution.ExpandingFhirPathEvaluator;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.test.SpringBootUnitTest;
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
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Reference;
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
class MultiFhirpathTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirEncoders encoders;

  @Nonnull
  Dataset<Row> evalExpression(@Nonnull final ObjectDataSource dataSource,
      @Nonnull final ResourceType subjectResource,
      @Nonnull final String fhirExpression) {

    return createEvaluator(subjectResource, dataSource)
        .evaluate(fhirExpression)
        .toIdValueDataset();
  }
  
  @Nonnull
  ExpandingFhirPathEvaluator createEvaluator(@Nonnull final ResourceType subjectResource,
      @Nonnull final DataSource datasource) {
    return new ExpandingFhirPathEvaluator(subjectResource, encoders.getContext(),
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

    final Dataset<Row> result = evalExpression(dataSource, ResourceType.PATIENT,
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
                .setId("Condition/y"),
            new Condition()
                .setSubject(new Reference("Patient/2"))
                .setCode(
                    new CodeableConcept()
                        .addCoding(new Coding().setCode("code-zx"))
                        .addCoding(new Coding().setCode("code-zy"))
                        .addCoding(new Coding().setCode("code-zz"))
                        .setText("Coding-z")
                )
                .setId("Condition/z")
        ));
  }
}
