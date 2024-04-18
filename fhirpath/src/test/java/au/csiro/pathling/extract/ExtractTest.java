package au.csiro.pathling.extract;

import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.query.ExpressionWithLabel;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import ca.uhn.fhir.context.FhirContext;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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
    patient.addName().setFamily("Smith").addGiven("John");
    patient.addName().setFamily("Tyler").addGiven("Mary");
    patient.addTelecom().setValue("555-555-5555");
    final ObjectDataSource dataSource = new ObjectDataSource(spark, encoders,
        List.of(patient));
    final ExtractQueryExecutor executor = new ExtractQueryExecutor(
        QueryConfiguration.builder().build(), fhirContext, spark, dataSource,
        Optional.ofNullable(terminologyServiceFactory));

    final ExtractRequest request = new ExtractRequest(ResourceType.PATIENT,
        List.of(
            ExpressionWithLabel.of("id", "id"),
            ExpressionWithLabel.of("name.given", "given_name"),
            ExpressionWithLabel.of("name.family", "family_name")
        ), Collections.emptyList(), Optional.empty());

    final Dataset<Row> result = executor.buildQuery(request);
    result.show();
  }

}
