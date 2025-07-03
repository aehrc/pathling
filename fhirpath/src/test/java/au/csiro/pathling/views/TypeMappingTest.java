package au.csiro.pathling.views;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Base64BinaryType;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CanonicalType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.DecimalType;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.MarkdownType;
import org.hl7.fhir.r4.model.OidType;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.PositiveIntType;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.TimeType;
import org.hl7.fhir.r4.model.Type;
import org.hl7.fhir.r4.model.UnsignedIntType;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.UrlType;
import org.hl7.fhir.r4.model.UuidType;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

@SpringBootUnitTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TypeMappingTest {

  @Autowired
  private FhirEncoders fhirEncoders;

  @Autowired
  private SparkSession sparkSession;

  /**
   * Provides test cases for type mapping tests. Each argument contains: 1. Description of the test
   * case 2. FHIR type instance with a value 3. Expected Spark SQL DataType 4. Expected value after
   * conversion
   */
  Stream<Arguments> typeMapppingTestCases() {
    return Stream.of(
        Arguments.of("base64Binary", new Base64BinaryType("SGVsbG8="), DataTypes.BinaryType,
            "Hello".getBytes()),
        Arguments.of("boolean", new BooleanType(true), DataTypes.BooleanType, true),
        Arguments.of("canonical", new CanonicalType("http://example.org/fhir/ValueSet/123"),
            DataTypes.StringType, "http://example.org/fhir/ValueSet/123"),
        Arguments.of("code", new CodeType("codeValue"), DataTypes.StringType, "codeValue"),
        Arguments.of("date", new DateType("2023-01-01"), DataTypes.StringType, "2023-01-01"),
        Arguments.of("dateTime", new DateTimeType("2023-01-01T12:00:00Z"), DataTypes.StringType,
            "2023-01-01T12:00:00Z"),
        Arguments.of("decimal", new DecimalType("123.45"), DataTypes.StringType, "123.45"),
        Arguments.of("id", new IdType("identifier123"), DataTypes.StringType, "identifier123"),
        Arguments.of("instant", new InstantType("2023-01-01T12:00:00Z"), DataTypes.StringType,
            "2023-01-01T12:00:00Z"),
        Arguments.of("integer", new IntegerType(42), DataTypes.IntegerType, 42),
        Arguments.of("markdown", new MarkdownType("**bold text**"), DataTypes.StringType,
            "**bold text**"),
        Arguments.of("oid", new OidType("1.2.3.4.5"), DataTypes.StringType, "1.2.3.4.5"),
        Arguments.of("positiveInt", new PositiveIntType(10), DataTypes.IntegerType, 10),
        Arguments.of("string", new StringType("text value"), DataTypes.StringType, "text value"),
        Arguments.of("time", new TimeType("12:00:00"), DataTypes.StringType, "12:00:00"),
        Arguments.of("unsignedInt", new UnsignedIntType(100), DataTypes.IntegerType, 100),
        Arguments.of("uri", new UriType("http://example.org"), DataTypes.StringType,
            "http://example.org"),
        Arguments.of("url", new UrlType("http://example.org/resource"), DataTypes.StringType,
            "http://example.org/resource"),
        Arguments.of("uuid", new UuidType("123e4567-e89b-12d3-a456-426614174000"),
            DataTypes.StringType, "123e4567-e89b-12d3-a456-426614174000")
    );
  }

  /**
   * Tests the default type mappings between FHIR types and Spark SQL types.
   *
   * @param description Description of the test case
   * @param fhirType FHIR type instance with a value
   * @param expectedDataType Expected Spark SQL DataType
   * @param expectedValue Expected value after conversion
   */
  @ParameterizedTest(name = "{0} type maps to {2}")
  @MethodSource("typeMapppingTestCases")
  void testDefaultTypeMappings(String description, Type fhirType, DataType expectedDataType,
      Object expectedValue) {
    final Resource patient = new Patient().setId("Patient/123");
    final ObjectDataSource dataSource = new ObjectDataSource(sparkSession, fhirEncoders,
        List.of(patient));
    final FhirViewExecutor executor = new FhirViewExecutor(fhirEncoders.getContext(), sparkSession,
        dataSource);

    final FhirView view = FhirView.withResource("Patient")
        .constants(
            ConstantDeclaration.builder().name("value").value(fhirType).build()
        )
        .selects(
            SelectClause.ofColumns(
                Column.single("value", "%value")
            )
        )
        .build();

    final Dataset<Row> result = executor.buildQuery(view);
    assertEquals(expectedDataType, result.schema().apply("value").dataType());

    Object actualValue = result.first().getAs("value");
    if (expectedDataType == DataTypes.BinaryType && expectedValue instanceof byte[]) {
      // For binary types, compare the string representation
      assertArrayEquals((byte[]) expectedValue, (byte[]) actualValue);
    } else {
      assertEquals(expectedValue, actualValue);
    }
  }
}
