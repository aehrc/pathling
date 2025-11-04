package au.csiro.pathling.fhirpath.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class ConversionFactorTest {

  
  @Test
  void testInverseProducesExactResult() {
    assertEquals(BigDecimal.ONE, ConversionFactor.inverseOf(new BigDecimal(12)).apply(new BigDecimal(12)));
  }


  @Test
  void testFractionProducesExactResult() {
    assertEquals(new BigDecimal(4), ConversionFactor.ofFraction(new BigDecimal(2), new BigDecimal(3)).apply(new BigDecimal(6)));
  }
}
