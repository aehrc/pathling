package au.csiro.pathling.views;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import jakarta.validation.ConstraintViolationException;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

class FhirViewExecutorTest {

  @Test
  void failsWhenInvalidView() {
    // Create an invalid view with an empty select list
    final FhirView view = FhirView.ofResource("Patient")
        .build();

    final FhirViewExecutor executor = new FhirViewExecutor(
        mock(FhirContext.class),
        mock(SparkSession.class),
        mock(DataSource.class));

    final ConstraintViolationException ex = assertThrows(
        ConstraintViolationException.class,
        () -> executor.buildQuery(view));

    assertEquals(
        "Valid SQL on FHIR view: select: must not be empty",
        ex.getMessage());
  }
}
