package au.csiro.pathling.views;

import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.validation.ValidationUtils;
import jakarta.validation.ConstraintViolation;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class FhirViewValidationTest {
  
  @Test
  public void testPassesForCompatibleUnionColumns() {
    final FhirView fhirView = new FhirView();
    fhirView.setResource("Patient");

    // Create a union with compatible columns
    Column compatibleColumn1 = Column.single("name", "Patient.name");
    Column compatibleColumn2 = Column.single("name", "Patient.gender");

    fhirView.setSelect(
        List.of(
            ColumnSelect.builder()
                .columns(Column.single("id", "Patient.id"))
                .unionsAll(
                    ColumnSelect.builder().columns(compatibleColumn1).build(),
                    ColumnSelect.builder().columns(compatibleColumn2).build()
                ).build()
        )
    );

    final Set<ConstraintViolation<FhirView>> validationResult = ValidationUtils.validate(fhirView);
    assertEquals(0, validationResult.size(),
        "Should pass validation with compatible union columns");
  }

  @Test
  public void testFailsForIncompatibleUnionColumns() {
    final FhirView fhirView = new FhirView();
    fhirView.setResource("Patient");

    // Create a union with incompatible columns (different collection indicators)
    Column compatibleColumn1 = Column.single("name", "Patient.name");
    Column compatibleColumn2 = Column.single("gender", "Patient.gender");
    Column incompatibleColumn = Column.builder()
        .name("name")
        .path("Patient.name")
        .collection(true) // This makes it incompatible with the others
        .build();

    fhirView.setSelect(
        List.of(
            ColumnSelect.builder()
                .columns(Column.single("id", "Patient.id"))
                .unionsAll(
                    ColumnSelect.builder().columns(compatibleColumn1, compatibleColumn2).build(),
                    ColumnSelect.builder().columns(compatibleColumn1, compatibleColumn2).build(),
                    ColumnSelect.builder().columns(incompatibleColumn, compatibleColumn2).build()
                ).build()
        )
    );
    final Set<ConstraintViolation<FhirView>> validationResult = ValidationUtils.validate(fhirView);
    System.out.println(validationResult);
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
    final FhirView fhirView = new FhirView();
    fhirView.setResource("Patient");
    fhirView.setSelect(
        List.of(
            ColumnSelect.builder()
                .columns(Column.single("id", "Patient.id"))
                .build()
        )
    );
    
    // Set a where clause with a null expression (which violates @NotNull)
    WhereClause invalidWhereClause = new WhereClause();
    invalidWhereClause.setDescription("This has a null expression");
    fhirView.setWhere(List.of(invalidWhereClause));
    
    final Set<ConstraintViolation<FhirView>> validationResult = ValidationUtils.validate(fhirView);
    assertEquals(1, validationResult.size());
    final ConstraintViolation<FhirView> violation = validationResult.iterator().next();
    assertEquals("must not be null", violation.getMessage());
    assertEquals("expression", violation.getPropertyPath().toString().split("\\.")[1]);
  }

  @Test
  public void testFailsForDuplicateColumnNames() {
    final FhirView fhirView = new FhirView();
    fhirView.setResource("Patient");
    fhirView.setSelect(
        List.of(
            ColumnSelect.builder()
                .columns(
                    Column.single("unique1", "Patient.name"),
                    Column.single("duplicate1", "Patient.name"),
                    Column.single("duplicate1", "Patient.name"),
                    Column.single("duplicate3", "Patient.name"),
                    Column.single("duplicate4", "Patient.name"),
                    Column.single("duplicate6", "Patient.name")
                )
                .selects(
                    ColumnSelect.builder().columns(
                        Column.single("unique3", "Patient.name"),
                        Column.single("duplicate4", "Patient.name"),
                        Column.single("duplicate5", "Patient.name")
                    ).build()
                ).build(),
            ColumnSelect.builder()
                .columns(
                    Column.single("unique2", "Patient.name"),
                    Column.single("duplicate2", "Patient.name"),
                    Column.single("duplicate2", "Patient.name"),
                    Column.single("duplicate3", "Patient.name"),
                    Column.single("duplicate5", "Patient.name")
                )
                .unionsAll(
                    ColumnSelect.builder().columns(
                        Column.single("unique4", "Patient.name"),
                        Column.single("duplicate6", "Patient.name")
                    ).build(),
                    ColumnSelect.builder().columns(
                        Column.single("unique4", "Patient.name"),
                        Column.single("duplicate6", "Patient.name")
                    ).build()
                ).build()
        )
    );
    final Set<ConstraintViolation<FhirView>> validationResult = ValidationUtils.validate(fhirView);
    final ConstraintViolation<FhirView> violation = validationResult.iterator().next();
    assertEquals(
        "Duplicate column names found: duplicate1, duplicate2, duplicate3, duplicate4, duplicate5, duplicate6",
        violation.getMessage());
    assertEquals(fhirView, violation.getRootBean());
  }
}
