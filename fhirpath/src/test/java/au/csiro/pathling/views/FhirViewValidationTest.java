package au.csiro.pathling.views;

import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.validation.ValidationUtils;
import jakarta.validation.ConstraintViolation;
import java.util.Set;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class FhirViewValidationTest {

  @Test
  public void failsWithNullColumnName() {
    final Column columnWithNoName = Column.builder()
        .path("Patient.id")
        .build(); // No name set, should fail validation

    // Create a valid FhirView with a single select clause
    final FhirView fhirView = FhirView.withResource("Patient")
        .selects(SelectClause.ofColumns(columnWithNoName))
        .build();

    final Set<ConstraintViolation<FhirView>> validationResult = ValidationUtils.validate(fhirView);
    assertEquals(1, validationResult.size());
    final ConstraintViolation<FhirView> violation = validationResult.iterator().next();
    assertEquals("must not be null", violation.getMessage());
    assertEquals("select[0].column[0].name", violation.getPropertyPath().toString());
  }

  @Test
  public void testPassesForCompatibleUnionColumns() {
    // Create a union with compatible columns
    Column compatibleColumn1 = Column.single("name", "Patient.name");
    Column compatibleColumn2 = Column.single("name", "Patient.gender");

    final FhirView fhirView = FhirView.withResource("Patient")
        .selects(
            SelectClause.builder().columns(
                Column.single("id", "Patient.id")
            ).unionsAll(
                SelectClause.ofColumns(compatibleColumn1),
                SelectClause.ofColumns(compatibleColumn2)
            ).build()
        ).build();

    final Set<ConstraintViolation<FhirView>> validationResult = ValidationUtils.validate(fhirView);
    assertEquals(0, validationResult.size(),
        "Should pass validation with compatible union columns");
  }

  @Test
  public void testFailsForIncompatibleUnionColumns() {
    // Create a union with incompatible columns (different collection indicators)
    Column compatibleColumn1 = Column.single("name", "Patient.name");
    Column compatibleColumn2 = Column.single("gender", "Patient.gender");
    Column incompatibleColumn = Column.builder()
        .name("name")
        .path("Patient.name")
        .collection(true) // This makes it incompatible with the others
        .build();

    final FhirView fhirView = FhirView.withResource("Patient")
        .selects(
            SelectClause.builder().columns(
                Column.single("id", "Patient.id")
            ).unionsAll(
                SelectClause.ofColumns(compatibleColumn1, compatibleColumn2),
                SelectClause.ofColumns(compatibleColumn1, compatibleColumn2),
                SelectClause.ofColumns(incompatibleColumn, compatibleColumn2)
            ).build()
        ).build();

    final Set<ConstraintViolation<FhirView>> validationResult = ValidationUtils.validate(fhirView);
    assertEquals(1, validationResult.size());
    final ConstraintViolation<FhirView> violation = validationResult.iterator().next();
    assertEquals(
        "Incompatible columns found in unionAll element at index 2: "
            + "expected [Column(name=name, path=Patient.name, description=null, collection=false, type=null), "
            + "Column(name=gender, path=Patient.gender, description=null, collection=false, type=null)] "
            + "but found [Column(name=name, path=Patient.name, description=null, collection=true, type=null), "
            + "Column(name=gender, path=Patient.gender, description=null, collection=false, type=null)]",
        violation.getMessage());
    assertEquals(fhirView, violation.getRootBean());
  }

  @Test
  public void testValidatesWhereClause() {
    // Set a where clause with a null expression (which violates @NotNull)
    WhereClause invalidWhereClause = new WhereClause();
    invalidWhereClause.setDescription("This has a null expression");

    final FhirView fhirView = FhirView.withResource("Patient")
        .selects(SelectClause.ofColumns(Column.single("id", "Patient.id")))
        .wheres(invalidWhereClause)
        .build();

    final Set<ConstraintViolation<FhirView>> validationResult = ValidationUtils.validate(fhirView);
    assertEquals(1, validationResult.size());
    final ConstraintViolation<FhirView> violation = validationResult.iterator().next();
    assertEquals("must not be null", violation.getMessage());
    assertEquals("expression", violation.getPropertyPath().toString().split("\\.")[1]);
  }

  @Test
  public void testFailsForDuplicateColumnNames() {
    final FhirView fhirView = FhirView.withResource("Patient")
        .selects(
            SelectClause.builder().columns(
                Column.single("unique1", "Patient.name"),
                Column.single("duplicate1", "Patient.name"),
                Column.single("duplicate1", "Patient.name"),
                Column.single("duplicate3", "Patient.name"),
                Column.single("duplicate4", "Patient.name"),
                Column.single("duplicate6", "Patient.name")
            ).selects(
                SelectClause.ofColumns(
                    Column.single("unique3", "Patient.name"),
                    Column.single("duplicate4", "Patient.name"),
                    Column.single("duplicate5", "Patient.name")
                )
            ).build(),
            SelectClause.builder().columns(
                Column.single("unique2", "Patient.name"),
                Column.single("duplicate2", "Patient.name"),
                Column.single("duplicate2", "Patient.name"),
                Column.single("duplicate3", "Patient.name"),
                Column.single("duplicate5", "Patient.name")
            ).unionsAll(
                SelectClause.ofColumns(
                    Column.single("unique4", "Patient.name"),
                    Column.single("duplicate6", "Patient.name")
                ),
                SelectClause.ofColumns(
                    Column.single("unique4", "Patient.name"),
                    Column.single("duplicate6", "Patient.name")
                )
            ).build()
        ).build();

    final Set<ConstraintViolation<FhirView>> validationResult = ValidationUtils.validate(fhirView);
    final ConstraintViolation<FhirView> violation = validationResult.iterator().next();
    assertEquals(
        "Duplicate column names found: duplicate1, duplicate2, duplicate3, duplicate4, duplicate5, duplicate6",
        violation.getMessage());
    assertEquals(fhirView, violation.getRootBean());
  }

  @Test
  public void testConstantNameValidation() {
    // Create a constant with an invalid name (contains hyphens)
    ConstantDeclaration invalidConstant = ConstantDeclaration.builder()
        .name("invalid-constant-name")
        .value(new StringType("test value"))
        .build();

    final FhirView fhirView = FhirView.withResource("Patient")
        .selects(SelectClause.ofColumns(Column.single("id", "Patient.id")))
        .constants(invalidConstant)
        .build();

    final Set<ConstraintViolation<FhirView>> validationResult = ValidationUtils.validate(fhirView);
    assertEquals(1, validationResult.size());
    final ConstraintViolation<FhirView> violation = validationResult.iterator().next();
    assertEquals("must be a valid name ([A-Za-z][A-Za-z0-9_]*)", violation.getMessage());
    assertEquals("constant[0].name", violation.getPropertyPath().toString());
  }
  
  @Test
  public void testAtMostOneNonNullForEachFields() {
    // Create a SelectClause with both forEach and forEachOrNull set
    SelectClause invalidSelectClause = SelectClause.builder()
        .forEach("Patient.name")
        .forEachOrNull("Patient.address")
        .columns(Column.single("id", "Patient.id"))
        .build();

    final FhirView fhirView = FhirView.withResource("Patient")
        .selects(invalidSelectClause)
        .build();

    final Set<ConstraintViolation<FhirView>> validationResult = ValidationUtils.validate(fhirView);
    assertEquals(1, validationResult.size());
    final ConstraintViolation<FhirView> violation = validationResult.iterator().next();
    assertEquals("Only one of the fields [forEach, forEachOrNull] can be non-null", violation.getMessage());
    assertEquals("select[0]", violation.getPropertyPath().toString());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("recursiveValidationTestCases")
  public void testRecursiveValidationOfColumnConstraints(String testName, FhirView view,
      String expectedPath) {
    Set<ConstraintViolation<FhirView>> validationResult = ValidationUtils.validate(view);
    assertEquals(1, validationResult.size());
    ConstraintViolation<FhirView> violation = validationResult.iterator().next();
    assertEquals("must be a valid name ([A-Za-z][A-Za-z0-9_]*)", violation.getMessage());
    assertEquals(expectedPath, violation.getPropertyPath().toString());
  }

  static Stream<Arguments> recursiveValidationTestCases() {
    // Create a column with an invalid name (doesn't match pattern)
    Column invalidColumn = Column.builder()
        .name("invalid-name-with-hyphens")  // Invalid: contains hyphens
        .path("Patient.name")
        .build();

    return Stream.of(
        // Test validation in SelectClause
        Arguments.of(
            "SelectClause direct validation",
            FhirView.withResource("Patient")
                .selects(SelectClause.ofColumns(invalidColumn))
                .build(),
            "select[0].column[0].name"
        ),

        // Test validation in ForEachSelect
        Arguments.of(
            "ForEachSelect direct validation",
            FhirView.withResource("Patient")
                .selects(SelectClause.forEach("Patient.name", invalidColumn))
                .build(),
            "select[0].column[0].name"
        ),

        // Test validation in ForEachOrNullSelect
        Arguments.of(
            "ForEachOrNullSelect direct validation",
            FhirView.withResource("Patient")
                .selects(SelectClause.forEach("Patient.name", invalidColumn))
                .build(),
            "select[0].column[0].name"
        ),

        // Test validation in nested structures (select within select)
        Arguments.of(
            "Nested select validation",
            FhirView.withResource("Patient")
                .selects(
                    SelectClause.builder().columns(Column.single("valid", "Patient.id"))
                        .selects(SelectClause.ofColumns(invalidColumn))
                        .build()
                )
                .build(),
            "select[0].select[0].column[0].name"
        ),

        // Test validation in unionAll
        Arguments.of(
            "UnionAll validation",
            FhirView.withResource("Patient")
                .selects(
                    SelectClause.builder().columns(Column.single("valid", "Patient.id"))
                        .unionsAll(SelectClause.ofColumns(invalidColumn))
                        .build()
                )
                .build(),
            "select[0].unionAll[0].column[0].name"
        )
    );
  }
}
