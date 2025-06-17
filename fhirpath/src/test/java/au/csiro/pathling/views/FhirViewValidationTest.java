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
            ColumnSelect.of(null,
                List.of(
                    Column.of("unique1", "Patient.name", "Patient name", false, "string"),
                    Column.of("duplicate1", "Patient.name", "Patient name", false, "string"),
                    Column.of("duplicate1", "Patient.name", "Patient name", false, "string"),
                    Column.of("duplicate3", "Patient.name", "Patient name", false, "string"),
                    Column.of("duplicate4", "Patient.name", "Patient name", false, "string")
                ),
                List.of(
                    ColumnSelect.of(null, List.of(
                        Column.of("unique3", "Patient.name", "Patient name", false, "string"),
                        Column.of("duplicate4", "Patient.name", "Patient name", false, "string"),
                        Column.of("duplicate5", "Patient.name", "Patient name", false, "string")
                    ), List.of(), List.of())
                ), List.of()
            ),
            ColumnSelect.of(null,
                List.of(
                    Column.of("unique2", "Patient.name", "Patient name", false, "string"),
                    Column.of("duplicate2", "Patient.name", "Patient name", false, "string"),
                    Column.of("duplicate2", "Patient.name", "Patient name", false, "string"),
                    Column.of("duplicate3", "Patient.name", "Patient name", false, "string"),
                    Column.of("duplicate5", "Patient.name", "Patient name", false, "string")
                ),
                List.of(), List.of()
            )
        )
    );
    final Set<ConstraintViolation<FhirView>> validationResult = ValidationUtils.validate(fhirView);
    final ConstraintViolation<FhirView> violation = validationResult.iterator().next();
    assertEquals(
        "Duplicate column names found: duplicate1, duplicate2, duplicate3, duplicate4, duplicate5",
        violation.getMessage());
    assertEquals(fhirView, violation.getRootBean());
  }
}
