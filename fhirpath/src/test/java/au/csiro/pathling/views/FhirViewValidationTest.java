package au.csiro.pathling.views;

import au.csiro.pathling.validation.ValidationUtils;
import jakarta.validation.ConstraintViolation;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FhirViewValidationTest {

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
