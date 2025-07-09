package au.csiro.pathling.views;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import au.csiro.pathling.views.Column.ColumnBuilder;
import au.csiro.pathling.views.FhirView.FhirViewBuilder;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.ExtendedAnalysisException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.r4.model.Base64BinaryType;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CanonicalType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.DecimalType;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.MarkdownType;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.OidType;
import org.hl7.fhir.r4.model.PositiveIntType;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.TimeType;
import org.hl7.fhir.r4.model.Type;
import org.hl7.fhir.r4.model.UnsignedIntType;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.UrlType;
import org.hl7.fhir.r4.model.UuidType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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


  private FhirViewExecutor fhirViewExecutor;

  @BeforeAll
  void setUp() {
    final Resource observation = new Observation()
        .setIssuedElement(new InstantType("2023-01-01T12:00:00+10:00"))
        .setValue(new Quantity(23.4))
        .setCode(
            new CodeableConcept()
                .setText("Test Observation")
                .addCoding(new Coding("http://example.org/fhir/CodeSystem/test", "test-code1",
                    "Test Code1"))
                .addCoding(new Coding("http://example.org/fhir/CodeSystem/test", "test-code2",
                    "Test Code2"))
        ).setId("Observation/123");
    final ObjectDataSource dataSource = new ObjectDataSource(sparkSession, fhirEncoders,
        List.of(observation));
    fhirViewExecutor = new FhirViewExecutor(fhirEncoders.getContext(), sparkSession,
        dataSource);
    System.out.println("Setting up TypeMappingTest with FhirViewExecutor");
  }

  @Value
  @Builder
  static class TestView {

    @Nullable
    @Builder.Default
    IBase constValue = null;

    @Nonnull
    @Builder.Default
    String expression = "%constValue";

    @Builder.Default
    boolean collection = false;

    @Nullable
    @Builder.Default
    String ansiType = null;


    @Nonnull
    FhirView create() {
      FhirViewBuilder viewBuilder = FhirView.withResource("Observation");
      if (constValue != null) {
        viewBuilder = viewBuilder.constants(
            ConstantDeclaration.builder().name("constValue")
                .value(Objects.requireNonNull(constValue)).build()
        );
      }

      ColumnBuilder columnBuilder = Column.builder()
          .name("value")
          .path(expression)
          .collection(collection);

      if (ansiType != null) {
        columnBuilder = columnBuilder.tag(
            List.of(ColumnTag.of("ansi/type", Objects.requireNonNull(ansiType))));
      }

      return viewBuilder.selects(
          SelectClause.ofColumns(
              columnBuilder.build()
          )
      ).build();
    }

    @Nonnull
    static TestView singleValue(@Nonnull String expression) {
      return TestView.builder()
          .expression(expression)
          .collection(false)
          .build();
    }

    @Nonnull
    static TestView collectionValue(@Nonnull String expression) {
      return TestView.builder()
          .expression(expression)
          .collection(true)
          .build();
    }
  }

  String makeArrayStr(@Nonnull Object... values) {
    return "WrappedArray(" + String.join(", ", Stream.of(values)
        .map(Objects::toString)
        .toArray(String[]::new)) + ")";
  }


  @Nonnull
  Dataset<Row> evalView(@Nonnull final TestView testView) {
    return fhirViewExecutor.buildQuery(testView.create());
  }

  @Nullable
  String evalToStrValue(@Nonnull final TestView testView,
      @Nonnull final DataType expectedDataType) {
    final Dataset<Row> resultDataset = evalView(testView);
    assertEquals(1, resultDataset.count(), "Expected exactly one row in the result");
    final DataType actualDataType = resultDataset.schema().apply(0).dataType();
    assertEquals(expectedDataType, actualDataType, "Unexpected data type for the column");
    final Row resultRow = resultDataset.first();
    return Optional.ofNullable(resultRow.isNullAt(0)
                               ? null
                               : resultRow.get(0))
        .map(obj -> obj instanceof byte[]
                    ? new String((byte[]) obj)
                    : obj.toString())
        .orElse(null);
  }

  /**
   * Provides test cases for type mapping tests. Each argument contains: 1. Description of the test
   * case 2. FHIR type instance with a value 3. Expected Spark SQL DataType 4. Expected value after
   * conversion
   */
  Stream<Arguments> fhirDefaultMappings() {
    return Stream.of(
        Arguments.of("base64Binary", new Base64BinaryType("SGVsbG8="), DataTypes.BinaryType,
            "Hello"),
        Arguments.of("boolean", new BooleanType(true), DataTypes.BooleanType, "true"),
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
        Arguments.of("integer", new IntegerType(42), DataTypes.IntegerType, "42"),
        Arguments.of("markdown", new MarkdownType("**bold text**"), DataTypes.StringType,
            "**bold text**"),
        Arguments.of("oid", new OidType("1.2.3.4.5"), DataTypes.StringType, "1.2.3.4.5"),
        Arguments.of("positiveInt", new PositiveIntType(10), DataTypes.IntegerType, "10"),
        Arguments.of("string", new StringType("text value"), DataTypes.StringType, "text value"),
        Arguments.of("time", new TimeType("12:00:00"), DataTypes.StringType, "12:00:00"),
        Arguments.of("unsignedInt", new UnsignedIntType(100), DataTypes.IntegerType, "100"),
        Arguments.of("uri", new UriType("http://example.org"), DataTypes.StringType,
            "http://example.org"),
        Arguments.of("url", new UrlType("http://example.org/resource"), DataTypes.StringType,
            "http://example.org/resource"),
        Arguments.of("uuid", new UuidType("123e4567-e89b-12d3-a456-426614174000"),
            DataTypes.StringType, "123e4567-e89b-12d3-a456-426614174000")
    );
  }

  @ParameterizedTest(name = "{0} type maps to {2}")
  @MethodSource("fhirDefaultMappings")
  void defaultSingleFhirMappings(String ignoreDescription, Type fhirType, DataType expectedDataType,
      String expectedValue) {
    Object actualValue = evalToStrValue(TestView.builder().constValue(fhirType).build(),
        expectedDataType);
    assertEquals(expectedValue, actualValue);
  }


  @ParameterizedTest(name = "collection {0} type maps to {2}")
  @MethodSource("fhirDefaultMappings")
  void defaultCollectionFhirMappings(String ignoreDescription, Type fhirType,
      DataType expectedDataType,
      String expectedValue) {
    Object actualValue = evalToStrValue(TestView.builder()
            .constValue(fhirType)
            .collection(true)
            .build(),
        DataTypes.createArrayType(expectedDataType, false));
    assertEquals(makeArrayStr(expectedValue), actualValue);
  }


  Stream<Arguments> fhirpathDefaultMappings() {
    return Stream.of(
        Arguments.of("String", "'Hello'", DataTypes.StringType, "Hello"),
        Arguments.of("Integer", "123", DataTypes.IntegerType, "123"),
        Arguments.of("Boolean", "true", DataTypes.BooleanType, "true"),
        Arguments.of("Decimal", "23.4", DataTypes.StringType, "23.4")
    );
  }
  
  @ParameterizedTest(name = "{0} type maps to {2}")
  @MethodSource("fhirpathDefaultMappings")
  void defaultSingleFhirpathMappings(String ignoreDescription, String literalExpr,
      DataType expectedDataType,
      String expectedValue) {
    String actualValue = evalToStrValue(TestView.builder().expression(literalExpr).build(),
        expectedDataType);
    assertEquals(expectedValue, actualValue);
  }

  @ParameterizedTest(name = "collection {0} type maps to {2}")
  @MethodSource("fhirpathDefaultMappings")
  void defaultCollectionFhirpathMappings(String ignoreDescription, String literalExpr,
      DataType expectedDataType,
      String expectedValue) {
    String actualValue = evalToStrValue(
        TestView.builder().expression(literalExpr).collection(true).build(),
        DataTypes.createArrayType(expectedDataType, false));
    assertEquals(makeArrayStr(expectedValue), actualValue);
  }


  Stream<Arguments> miscDefaultMappings() {
    return Stream.of(
        Arguments.of("empty", "{}", false, DataTypes.NullType, null),
        Arguments.of("empty as collection", "{}", true, DataTypes.NullType, null),
        Arguments.of("decimal", "value.ofType(Quantity).value", false, DataTypes.StringType,
            "23.400000"),
        Arguments.of("instant", "issued", false, DataTypes.StringType,
            "2023-01-01T12:00:00+10:00"),
        Arguments.of("array of strings", "code.coding.code", true,
            DataTypes.createArrayType(DataTypes.StringType, true),
            makeArrayStr("test-code1", "test-code2"))
    );
  }

  @ParameterizedTest(name = "{0} type maps to {3}")
  @MethodSource("miscDefaultMappings")
  void defaultMiscMappings(String ignoreDescription, String expression,
      boolean collection, DataType expectedDataType, Object expectedValue) {
    Object actualValue = evalToStrValue(
        TestView.builder().expression(expression).collection(collection).build(),
        expectedDataType);
    assertEquals(expectedValue, actualValue);
  }


  Stream<Arguments> ansiLegalCasts() {
    return Stream.of(
        Arguments.of("VARCHAR(10)", "value", DataTypes.StringType, "value"),
        Arguments.of("INT", "123", DataTypes.IntegerType, "123")
    );
  }


  @ParameterizedTest(name = "{0} type maps to {2}")
  @MethodSource("ansiLegalCasts")
  void legalSingleAnsiCasts(String ansiType, String stringValue,
      DataType expectedDataType,
      String expectedValue) {
    Object actualValue = evalToStrValue(TestView.builder().
            constValue(new StringType(stringValue))
            .ansiType(ansiType)
            .build(),
        expectedDataType);
    assertEquals(expectedValue, actualValue);
  }


  @ParameterizedTest(name = "ARRAY of {0} type maps to {2}")
  @MethodSource("ansiLegalCasts")
  void legalCollectionAnsiCasts(String ansiType, String stringValue,
      DataType expectedDataType,
      String expectedValue) {
    Object actualValue = evalToStrValue(TestView.builder().
            constValue(new StringType(stringValue))
            .ansiType("ARRAY<" + ansiType + ">")
            .collection(true)
            .build(),
        DataTypes.createArrayType(expectedDataType));
    assertEquals(makeArrayStr(expectedValue), actualValue);
  }


  Stream<Arguments> ansiLenientCasts() {
    return Stream.of(
        Arguments.of("INT", "xxxx", DataTypes.IntegerType),
        Arguments.of("BOOLEAN", "xxxx", DataTypes.BooleanType)
    );
  }

  @ParameterizedTest(name = "{0} type maps to {2}")
  @MethodSource("ansiLenientCasts")
  void lenientAnsiCasts(String ansiType, String stringValue,
      DataType expectedDataType) {
    Object actualValue = evalToStrValue(TestView.builder().
            constValue(new StringType(stringValue))
            .ansiType(ansiType)
            .build(),
        expectedDataType);
    assertNull(actualValue);
  }


  Stream<Arguments> ansiMiscCasts() {
    return Stream.of(
        Arguments.of("DECIMAL(10,3)", "value.ofType(Quantity).value",
            DataTypes.createDecimalType(10, 3), "23.400"),
        Arguments.of("TIMESTAMP WITHOUT TIME ZONE", "issued",
            DataTypes.TimestampNTZType, "2023-01-01T02:00"),
        Arguments.of("TIMESTAMP WITH TIME ZONE", "issued",
            DataTypes.TimestampType, "2023-01-01 02:00:00.0")
    );
  }

  @ParameterizedTest(name = "{0} type maps to {2}")
  @MethodSource("ansiMiscCasts")
  void miscAnsiCasts(String ansiType, String expression,
      DataType expectedDataType, String expectedValue) {
    Object actualValue = evalToStrValue(
        TestView.builder().expression(expression).ansiType(ansiType).build(),
        expectedDataType);
    assertEquals(expectedValue, actualValue);
  }


  Stream<Arguments> ansiFailingCasts() {
    return Stream.of(
        Arguments.of("ARRAY<INT>", new IntegerType(213), false,
            DataTypes.createArrayType(DataTypes.IntegerType)),
        Arguments.of("BOOLEAN", new BooleanType(true), true, DataTypes.BooleanType)
    );
  }

  @ParameterizedTest(name = "{0} type maps to {2}")
  @MethodSource("ansiFailingCasts")
  void failingAnsiCasts(String ansiType, Type value, boolean collection,
      DataType expectedDataType) {
    final ExtendedAnalysisException ex = assertThrows(ExtendedAnalysisException.class,
        () -> evalToStrValue(TestView.builder().
                constValue(value)
                .ansiType(ansiType)
                .collection(collection)
                .build(),
            expectedDataType));
  }

  @Test
  void singleStructTypesNotSupported() {
    final UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
        () -> evalView(TestView.singleValue("code.coding.first()")));
    assertEquals("Cannot obtain value for non-primitive collection of FHIR type: CODING",
        ex.getMessage());
  }

  @Test
  void collectionStructTypesNotSupported() {
    final UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
        () -> evalView(TestView.collectionValue("code.coding")));
    assertEquals("Cannot obtain value for non-primitive collection of FHIR type: CODING",
        ex.getMessage());
  }

}
  

