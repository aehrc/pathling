package au.csiro.pathling.views;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import ca.uhn.fhir.context.FhirContext;
import jakarta.validation.ConstraintViolationException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.MarkdownType;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Resource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

@SpringBootUnitTest
public class TypeMappingTest {

  @Autowired
  private FhirEncoders fhirEncoders;

  @Autowired
  private SparkSession sparkSession;

  
  
  //     | FHIR type    | Spark SQL type |
  //     |--------------|----------------|
  //     | base64Binary | BINARY         |
  //     | boolean      | BOOLEAN        |
  //     | canonical    | STRING         |
  //     | code         | STRING         |
  //     | date         | STRING         |
  //     | dateTime     | STRING         |
  //     | decimal      | STRING         |
  //     | id           | STRING         |
  //     | instant      | STRING         |
  //     | integer      | INT            |
  //     | integer64    | BIGINT         |
  //     | markdown     | STRING         |
  //     | oid          | STRING         |
  //     | positiveInt  | INT            |
  //     | string       | STRING         |
  //     | time         | STRING         |
  //     | unsignedInt  | INT            |
  //     | uri          | STRING         |
  //     | url          | STRING         |
  //     | uuid         | STRING         |

  @Test
  void testDefaultBooleanTypeMappings() {

    final Resource patient = new Patient().setId("Patient/123");
    final ObjectDataSource dataSource = new ObjectDataSource(sparkSession, fhirEncoders,
        List.of(patient));
    final FhirViewExecutor executor = new FhirViewExecutor(fhirEncoders.getContext(), sparkSession,
        dataSource);

    final FhirView view = FhirView.withResource("Patient")
        .constants(
            ConstantDeclaration.builder().name("value").value(new BooleanType(true)).build()
        )
        .selects(
            SelectClause.ofColumns(
                Column.single("value", "%value")
            )
        )
        .build();

    final Dataset<Row> result = executor.buildQuery(view);
    assertEquals(DataTypes.BooleanType, result.schema().apply("value").dataType());
    assertEquals(true, result.first().getAs("value"));
  }


  @Test
  void testDefaultCodeTypeMappings() {

    final Resource patient = new Patient().setId("Patient/123");
    final ObjectDataSource dataSource = new ObjectDataSource(sparkSession, fhirEncoders,
        List.of(patient));
    final FhirViewExecutor executor = new FhirViewExecutor(fhirEncoders.getContext(), sparkSession,
        dataSource);

    final FhirView view = FhirView.withResource("Patient")
        .constants(
            ConstantDeclaration.builder().name("value").value(new CodeType("codeValue")).build()
        )
        .selects(
            SelectClause.ofColumns(
                Column.single("value", "%value")
            )
        )
        .build();

    final Dataset<Row> result = executor.buildQuery(view);
    assertEquals(DataTypes.StringType, result.schema().apply("value").dataType());
    assertEquals("codeValue", result.first().getAs("value"));
  }
}
