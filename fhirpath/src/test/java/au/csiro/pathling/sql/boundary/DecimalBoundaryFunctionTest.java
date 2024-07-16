package au.csiro.pathling.sql.boundary;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DecimalBoundaryFunctionTest {

  @Test
  @Order(1)
  void lowBoundaryMaxPrecision() {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, null);
    log.debug("{}.lowBoundary() // {}", d, result);
    assertEquals(new BigDecimal("1.587"), result);
  }

  @Test
  @Order(2)
  void lowBoundaryContractedPrecision() {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 3);
    log.debug("{}.lowBoundary(3) // {}", d, result);
    assertEquals(new BigDecimal("1.58"), result);
  }

  @Test
  @Order(3)
  void lowBoundaryExpandedPrecision() {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 5);
    log.debug("{}.lowBoundary(5) // {}", d, result);
    assertEquals(new BigDecimal("1.587"), result);
  }

  @Test
  @Order(4)
  void negativePrecision() {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, -1);
    log.debug("{}.lowBoundary(-1) // {}", d, result);
    assertNull(result);
  }

  @Test
  @Order(5)
  void lowBoundaryNegativeMaxPrecision() {
    final BigDecimal d = new BigDecimal("-1.587");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, null);
    log.debug("{}.lowBoundary() // {}", d, result);
    assertEquals(new BigDecimal("-1.5879999999999999999999999999999999999"), result);
  }

  @Test
  @Order(6)
  void lowBoundaryNegativeContractedPrecision() {
    final BigDecimal d = new BigDecimal("-1.587");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 3);
    log.debug("{}.lowBoundary(3) // {}", d, result);
    assertEquals(new BigDecimal("-1.59"), result);
  }

  @Test
  @Order(7)
  void lowBoundaryNegativeExpandedPrecision() {
    final BigDecimal d = new BigDecimal("-1.587");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 5);
    log.debug("{}.lowBoundary(5) // {}", d, result);
    assertEquals(new BigDecimal("-1.5879"), result);
  }

  @Test
  @Order(8)
  void precisionHigherThanMax() {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 39);
    log.debug("{}.lowBoundary(39) // {}", d, result);
    assertNull(result);
  }

  @Test
  @Order(9)
  void lowBoundaryIntegerMaxPrecision() {
    final BigDecimal d = new BigDecimal("1");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, null);
    log.debug("{}.lowBoundary() // {}", d, result);
    assertEquals(new BigDecimal("1"), result);
  }

  @Test
  @Order(10)
  void lowBoundaryIntegerZeroPrecision() {
    final BigDecimal d = new BigDecimal("1");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 0);
    log.debug("{}.lowBoundary(0) // {}", d, result);
    assertNull(result);
  }

  @Test
  @Order(11)
  void lowBoundaryIntegerExpandedPrecision() {
    final BigDecimal d = new BigDecimal("1");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 5);
    log.debug("{}.lowBoundary(5) // {}", d, result);
    assertEquals(new BigDecimal("1"), result);
  }

  @Test
  @Order(12)
  void highBoundaryMaxPrecision() {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, null);
    log.debug("{}.highBoundary() // {}", d, result);
    assertEquals(new BigDecimal("1.5879999999999999999999999999999999999"), result);
  }

  @Test
  @Order(13)
  void highBoundaryContractedPrecision() {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, 3);
    log.debug("{}.highBoundary(3) // {}", d, result);
    assertEquals(new BigDecimal("1.59"), result);
  }

  @Test
  @Order(14)
  void highBoundaryExpandedPrecision() {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, 5);
    log.debug("{}.highBoundary(5) // {}", d, result);
    assertEquals(new BigDecimal("1.5879"), result);
  }

  @Test
  @Order(15)
  void highBoundaryNegativeMaxPrecision() {
    final BigDecimal d = new BigDecimal("-1.587");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, null);
    log.debug("{}.highBoundary() // {}", d, result);
    assertEquals(new BigDecimal("-1.587"), result);
  }

  @Test
  @Order(16)
  void highBoundaryNegativeContractedPrecision() {
    final BigDecimal d = new BigDecimal("-1.587");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, 3);
    log.debug("{}.highBoundary(3) // {}", d, result);
    assertEquals(new BigDecimal("-1.58"), result);
  }

  @Test
  @Order(17)
  void highBoundaryNegativeExpandedPrecision() {
    final BigDecimal d = new BigDecimal("-1.587");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, 5);
    log.debug("{}.highBoundary(5) // {}", d, result);
    assertEquals(new BigDecimal("-1.587"), result);
  }

  @Test
  @Order(18)
  void highBoundaryIntegerMaxPrecision() {
    final BigDecimal d = new BigDecimal("1");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, null);
    log.debug("{}.highBoundary() // {}", d, result);
    assertEquals(new BigDecimal("1.9999999999999999999999999999999999999"), result);
  }

  @Test
  @Order(19)
  void highBoundaryIntegerZeroPrecision() {
    final BigDecimal d = new BigDecimal("1");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, 0);
    log.debug("{}.highBoundary(0) // {}", d, result);
    assertNull(result);
  }

  @Test
  @Order(20)
  void highBoundaryIntegerExpandedPrecision() {
    final BigDecimal d = new BigDecimal("1");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, 5);
    log.debug("{}.highBoundary(5) // {}", d, result);
    assertEquals(new BigDecimal("1.9999"), result);
  }

  @Test
  @Order(21)
  void lowBoundaryTruncateFraction() {
    final BigDecimal d = new BigDecimal("12.587");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 2);
    log.debug("{}.lowBoundary(2) // {}", d, result);
    assertEquals(new BigDecimal("12"), result);
  }

  @Test
  @Order(22)
  void highBoundaryTruncateFraction() {
    final BigDecimal d = new BigDecimal("12.587");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, 2);
    log.debug("{}.highBoundary(2) // {}", d, result);
    assertEquals(new BigDecimal("13"), result);
  }

  @Test
  @Order(23)
  void lowBoundaryTrailingZeroes() {
    final BigDecimal d = new BigDecimal("12.500");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 4);
    log.debug("{}.lowBoundary(4) // {}", d, result);
    assertEquals(new BigDecimal("12.5"), result);
  }

  @Test
  @Order(24)
  void highBoundaryTrailingZeroes() {
    final BigDecimal d = new BigDecimal("12.500");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, 4);
    log.debug("{}.highBoundary(4) // {}", d, result);
    assertEquals(new BigDecimal("12.59"), result);
  }

  @Test
  @Order(25)
  void lowBoundaryInsignificantOnLeft() {
    final BigDecimal d = new BigDecimal("120");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 2);
    log.debug("{}.lowBoundary(2) // {}", d, result);
    assertEquals(new BigDecimal("120"), result);
  }

  @Test
  @Order(26)
  void highBoundaryInsignificantOnLeft() {
    final BigDecimal d = new BigDecimal("120");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, 2);
    log.debug("{}.highBoundary(2) // {}", d, result);
    assertEquals(new BigDecimal("130"), result);
  }

  @Test
  @Order(27)
  void lowBoundaryNegativeInsignificantOnLeft() {
    final BigDecimal d = new BigDecimal("-120");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 2);
    log.debug("{}.lowBoundary(2) // {}", d, result);
    assertEquals(new BigDecimal("-130"), result);
  }

  @Test
  @Order(28)
  void highBoundaryNegativeInsignificantOnLeft() {
    final BigDecimal d = new BigDecimal("-120");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, 2);
    log.debug("{}.highBoundary(2) // {}", d, result);
    assertEquals(new BigDecimal("-120"), result);
  }

  @Test
  @Order(29)
  void lowBoundaryLessThanOne() {
    final BigDecimal d = new BigDecimal("0.0034");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 1);
    log.debug("{}.lowBoundary(1) // {}", d, result);
    assertEquals(new BigDecimal("0.003"), result);
  }

  @Test
  @Order(30)
  void highBoundaryLessThanOne() {
    final BigDecimal d = new BigDecimal("0.0034");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, 1);
    log.debug("{}.highBoundary(1) // {}", d, result);
    assertEquals(new BigDecimal("0.004"), result);
  }

  @Test
  @Order(31)
  void lowBoundaryNegativeLessThanOne() {
    final BigDecimal d = new BigDecimal("-0.0034");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 1);
    log.debug("{}.lowBoundary(1) // {}", d, result);
    assertEquals(new BigDecimal("-0.004"), result);
  }

  @Test
  @Order(32)
  void highBoundaryNegativeLessThanOne() {
    final BigDecimal d = new BigDecimal("-0.0034");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, 1);
    log.debug("{}.highBoundary(1) // {}", d, result);
    assertEquals(new BigDecimal("-0.003"), result);
  }

}
