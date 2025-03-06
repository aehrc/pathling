package au.csiro.pathling.sql.misc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import au.csiro.pathling.encoders.terminology.ucum.Ucum;
import au.csiro.pathling.fhirpath.CalendarDurationUtils;
import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import java.math.BigDecimal;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.hl7.fhir.r4.model.Quantity;

public class QuantityToLiteralTest {

  private QuantityToLiteral quantityToLiteral;

  @BeforeEach
  public void setUp() {
    quantityToLiteral = new QuantityToLiteral();
  }

  @Test
  public void testCallWithNullRow() {
    assertNull(quantityToLiteral.call(null));
  }

  @Test
  public void testCallWithNullQuantity() {
    Row row = RowFactory.create(null, null, null, null, null, null, null, null, null, null);
    assertNull(quantityToLiteral.call(row));
  }

  @Test
  public void testCallWithUcumQuantity() {
    Quantity quantity = new Quantity();
    quantity.setValue(new BigDecimal("123.456"));
    quantity.setSystem(Ucum.SYSTEM_URI);
    quantity.setCode("mg");

    Row row = QuantityEncoding.encode(quantity);
    assertEquals("123.456 'mg'", quantityToLiteral.call(row));
  }

  @Test
  public void testCallWithTimeDurationQuantity() {
    Quantity quantity = new Quantity();
    quantity.setValue(new BigDecimal("5"));
    quantity.setSystem(CalendarDurationUtils.FHIRPATH_CALENDAR_DURATION_URI);
    quantity.setCode("day");

    Row row = QuantityEncoding.encode(quantity);
    assertEquals("5 day", quantityToLiteral.call(row));
  }

  @Test
  public void testCallWithNonUcumQuantity() {
    Quantity quantity = new Quantity();
    quantity.setValue(new BigDecimal("10"));
    quantity.setSystem("http://example.com");
    quantity.setCode("exampleUnit");

    Row row = QuantityEncoding.encode(quantity);
    assertNull(quantityToLiteral.call(row));
  }
}
