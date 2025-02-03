package au.csiro.pathling.extract;

import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.query.ExpressionWithLabel;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.assertions.DatasetAssert;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import ca.uhn.fhir.context.FhirContext;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

@SpringBootUnitTest
@Slf4j
class ExtractTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  @Autowired
  FhirEncoders encoders;

  @Autowired
  TerminologyServiceFactory terminologyServiceFactory;

  @Test
  void simple() {
    final Patient patient = new Patient();
    patient.setId("1");
    patient.addName().setFamily("Kay").addGiven("Awee");
    patient.addName().setFamily("Robert").addGiven("Zosia");
    final Patient.ContactComponent contact1 = patient.addContact();
    contact1.getName().setFamily("Ravindra").addGiven("Veremund");
    final Patient.ContactComponent contact2 = patient.addContact();
    contact2.getName().setFamily("Einion").addGiven("Kazuko");
    final ObjectDataSource dataSource = new ObjectDataSource(spark, encoders,
        List.of(patient));
    final ExtractQueryExecutor executor = new ExtractQueryExecutor(
        QueryConfiguration.builder().build(), fhirContext, spark, dataSource,
        Optional.ofNullable(terminologyServiceFactory));

    final ExtractRequest request = new ExtractRequest(ResourceType.PATIENT,
        List.of(
            ExpressionWithLabel.of("id", "id"),
            ExpressionWithLabel.of("name.given", "given_name"),
            ExpressionWithLabel.of("name.family", "family_name"),
            ExpressionWithLabel.of("contact.name.given", "contact_given_name"),
            ExpressionWithLabel.of("contact.name.family", "contact_family_name")
        ), Collections.emptyList(), Optional.empty());

    final Dataset<Row> result = executor.buildQuery(request);
    final Dataset<Row> expected = DatasetBuilder.of(spark)
        .withColumn("id", DataTypes.StringType)
        .withColumn("given_name", DataTypes.StringType)
        .withColumn("family_name", DataTypes.StringType)
        .withColumn("contact_given_name", DataTypes.StringType)
        .withColumn("contact_family_name", DataTypes.StringType)
        .withRow("1", "Awee", "Kay", "Veremund", "Ravindra")
        .withRow("1", "Awee", "Kay", "Kazuko", "Einion")
        .withRow("1", "Zosia", "Robert", "Veremund", "Ravindra")
        .withRow("1", "Zosia", "Robert", "Kazuko", "Einion")
        .build();

    new DatasetAssert(result)
        .hasRowsUnordered(expected);
  }

}
