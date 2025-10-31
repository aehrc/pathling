package au.csiro.pathling.fhirpath.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class ConversionFactorTest {

  
  @Test
  void testConversionFactor() {
    assertEquals(BigDecimal.ONE, ConversionFactor.inverseOf(new BigDecimal(12)).apply(new BigDecimal(12)));
  }
}
