package au.csiro.pathling.sql.types;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class FlexiDecimalTest {
  
  @Test
  void testNormalize() {
    // Normalize big decimal with negative scale
    assertEquals(new BigDecimal("123000"), FlexiDecimal.normalize(new BigDecimal("1.23E+5")));
    assertEquals(new BigDecimal("99900000000000000000000000000000000000"),
        FlexiDecimal.normalize(new BigDecimal("9.99E+37")));
    // To many significant digits --> null
    assertNull(FlexiDecimal.normalize(new BigDecimal("1.0E+38")));

    // normalize decimals to reduced precision
    assertEquals(new BigDecimal("0.010000000000000000000000000000000000000"),
        FlexiDecimal.normalize(new BigDecimal("0.010000000000000000000000000000000000000000000")));

    assertEquals(FlexiDecimal.MAX_PRECISION,
        FlexiDecimal.normalize(new BigDecimal("0.010000000000000000000000000000000000000000000"))
            .precision());

    assertEquals(new BigDecimal("0.015555555555555555555555555555555555556"),
        FlexiDecimal.normalize(new BigDecimal(
            "0.01555555555555555555555555555555555555555555555555555555555555555555555555")));
  }
}
