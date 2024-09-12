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
  void maxPrecision() {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal lowBoundary = DecimalBoundaryFunction.lowBoundaryForDecimal(d, null);
    final BigDecimal highBoundary = DecimalBoundaryFunction.highBoundaryForDecimal(d, null);
    log.debug("{}.lowBoundary() // {}", d, lowBoundary);
    log.debug("{}.highBoundary() // {}", d, highBoundary);
    assertEquals(new BigDecimal("1.587"), lowBoundary);
    assertEquals(new BigDecimal("1.5874999999999999999999999999999999999"), highBoundary);
  }

  @Test
  @Order(2)
  void contractedPrecision() {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal lowBoundary = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 3);
    final BigDecimal highBoundary = DecimalBoundaryFunction.highBoundaryForDecimal(d, 3);
    log.debug("{}.lowBoundary(3) // {}", d, lowBoundary);
    log.debug("{}.highBoundary(3) // {}", d, highBoundary);
    assertEquals(new BigDecimal("1.58"), lowBoundary);
    assertEquals(new BigDecimal("1.59"), highBoundary);
  }

  @Test
  @Order(3)
  void expandedPrecision() {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal lowBoundary = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 5);
    final BigDecimal highBoundary = DecimalBoundaryFunction.highBoundaryForDecimal(d, 5);
    log.debug("{}.lowBoundary(5) // {}", d, lowBoundary);
    log.debug("{}.highBoundary(5) // {}", d, highBoundary);
    assertEquals(new BigDecimal("1.587"), lowBoundary);
    assertEquals(new BigDecimal("1.5874"), highBoundary);
  }

  @Test
  @Order(4)
  void negativePrecision() {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal lowBoundary = DecimalBoundaryFunction.lowBoundaryForDecimal(d, -1);
    final BigDecimal highBoundary = DecimalBoundaryFunction.highBoundaryForDecimal(d, -1);
    log.debug("{}.lowBoundary(-1) // {}", d, lowBoundary);
    log.debug("{}.highBoundary(-1) // {}", d, highBoundary);
    assertNull(lowBoundary);
    assertNull(highBoundary);
  }

  @Test
  @Order(5)
  void negativeMaxPrecision() {
    final BigDecimal d = new BigDecimal("-1.587");
    final BigDecimal lowBoundary = DecimalBoundaryFunction.lowBoundaryForDecimal(d, null);
    final BigDecimal highBoundary = DecimalBoundaryFunction.highBoundaryForDecimal(d, null);
    log.debug("{}.lowBoundary() // {}", d, lowBoundary);
    log.debug("{}.highBoundary() // {}", d, highBoundary);
    assertEquals(new BigDecimal("-1.5874999999999999999999999999999999999"), lowBoundary);
    assertEquals(new BigDecimal("-1.587"), highBoundary);
  }

  @Test
  @Order(6)
  void negativeContractedPrecision() {
    final BigDecimal d = new BigDecimal("-1.587");
    final BigDecimal lowBoundary = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 3);
    final BigDecimal highBoundary = DecimalBoundaryFunction.highBoundaryForDecimal(d, 3);
    log.debug("{}.lowBoundary(3) // {}", d, lowBoundary);
    log.debug("{}.highBoundary(3) // {}", d, highBoundary);
    assertEquals(new BigDecimal("-1.59"), lowBoundary);
    assertEquals(new BigDecimal("-1.58"), highBoundary);
  }

  @Test
  @Order(7)
  void negativeExpandedPrecision() {
    final BigDecimal d = new BigDecimal("-1.587");
    final BigDecimal lowBoundary = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 5);
    final BigDecimal highBoundary = DecimalBoundaryFunction.highBoundaryForDecimal(d, 5);
    log.debug("{}.lowBoundary(5) // {}", d, lowBoundary);
    log.debug("{}.highBoundary(5) // {}", d, highBoundary);
    assertEquals(new BigDecimal("-1.5874"), lowBoundary);
    assertEquals(new BigDecimal("-1.587"), highBoundary);
  }

  @Test
  @Order(8)
  void precisionHigherThanMax() {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal lowBoundary = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 39);
    final BigDecimal highBoundary = DecimalBoundaryFunction.highBoundaryForDecimal(d, 39);
    log.debug("{}.lowBoundary(39) // {}", d, lowBoundary);
    log.debug("{}.highBoundary(39) // {}", d, highBoundary);
    assertNull(lowBoundary);
    assertNull(highBoundary);
  }

  @Test
  @Order(9)
  void integerMaxPrecision() {
    final BigDecimal d = new BigDecimal("1");
    final BigDecimal lowBoundary = DecimalBoundaryFunction.lowBoundaryForDecimal(d, null);
    final BigDecimal highBoundary = DecimalBoundaryFunction.highBoundaryForDecimal(d, null);
    log.debug("{}.lowBoundary() // {}", d, lowBoundary);
    log.debug("{}.highBoundary() // {}", d, highBoundary);
    assertEquals(new BigDecimal("1"), lowBoundary);
    assertEquals(new BigDecimal("1.4999999999999999999999999999999999999"), highBoundary);
  }

  @Test
  @Order(10)
  void integerZeroPrecision() {
    final BigDecimal d = new BigDecimal("1");
    final BigDecimal lowBoundary = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 0);
    final BigDecimal highBoundary = DecimalBoundaryFunction.highBoundaryForDecimal(d, 0);
    log.debug("{}.lowBoundary(0) // {}", d, lowBoundary);
    log.debug("{}.highBoundary(0) // {}", d, highBoundary);
    assertNull(lowBoundary);
    assertNull(highBoundary);
  }

  @Test
  @Order(11)
  void integerExpandedPrecision() {
    final BigDecimal d = new BigDecimal("1");
    final BigDecimal lowBoundary = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 5);
    final BigDecimal highBoundary = DecimalBoundaryFunction.highBoundaryForDecimal(d, 5);
    log.debug("{}.lowBoundary(5) // {}", d, lowBoundary);
    log.debug("{}.highBoundary(5) // {}", d, highBoundary);
    assertEquals(new BigDecimal("1"), lowBoundary);
    assertEquals(new BigDecimal("1.4999"), highBoundary);
  }

  @Test
  @Order(12)
  void truncateFraction() {
    final BigDecimal d = new BigDecimal("12.587");
    final BigDecimal lowBoundary = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 2);
    final BigDecimal highBoundary = DecimalBoundaryFunction.highBoundaryForDecimal(d, 2);
    log.debug("{}.lowBoundary(2) // {}", d, lowBoundary);
    log.debug("{}.highBoundary(2) // {}", d, highBoundary);
    assertEquals(new BigDecimal("12"), lowBoundary);
    assertEquals(new BigDecimal("13"), highBoundary);
  }

  @Test
  @Order(13)
  void trailingZeroes() {
    final BigDecimal d = new BigDecimal("12.500");
    final BigDecimal lowBoundary = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 4);
    final BigDecimal highBoundary = DecimalBoundaryFunction.highBoundaryForDecimal(d, 4);
    log.debug("{}.lowBoundary(4) // {}", d, lowBoundary);
    log.debug("{}.highBoundary(4) // {}", d, highBoundary);
    assertEquals(new BigDecimal("12.5"), lowBoundary);
    assertEquals(new BigDecimal("12.5"), highBoundary);
  }

  @Test
  @Order(14)
  void insignificantOnLeft() {
    final BigDecimal d = new BigDecimal("120");
    final BigDecimal lowBoundary = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 2);
    final BigDecimal highBoundary = DecimalBoundaryFunction.highBoundaryForDecimal(d, 2);
    log.debug("{}.lowBoundary(2) // {}", d, lowBoundary);
    log.debug("{}.highBoundary(2) // {}", d, highBoundary);
    assertEquals(new BigDecimal("120"), lowBoundary);
    assertEquals(new BigDecimal("130"), highBoundary);
  }

  @Test
  @Order(15)
  void negativeInsignificantOnLeft() {
    final BigDecimal d = new BigDecimal("-120");
    final BigDecimal lowBoundary = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 2);
    final BigDecimal highBoundary = DecimalBoundaryFunction.highBoundaryForDecimal(d, 2);
    log.debug("{}.lowBoundary(2) // {}", d, lowBoundary);
    log.debug("{}.highBoundary(2) // {}", d, highBoundary);
    assertEquals(new BigDecimal("-130"), lowBoundary);
    assertEquals(new BigDecimal("-120"), highBoundary);
  }

  @Test
  @Order(16)
  void lessThanOne() {
    final BigDecimal d = new BigDecimal("0.0034");
    final BigDecimal lowBoundary = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 1);
    final BigDecimal highBoundary = DecimalBoundaryFunction.highBoundaryForDecimal(d, 1);
    log.debug("{}.lowBoundary(1) // {}", d, lowBoundary);
    log.debug("{}.highBoundary(1) // {}", d, highBoundary);
    assertEquals(new BigDecimal("0.003"), lowBoundary);
    assertEquals(new BigDecimal("0.004"), highBoundary);
  }

  @Test
  @Order(17)
  void negativeLessThanOne() {
    final BigDecimal d = new BigDecimal("-0.0034");
    final BigDecimal lowBoundary = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 1);
    final BigDecimal highBoundary = DecimalBoundaryFunction.highBoundaryForDecimal(d, 1);
    log.debug("{}.lowBoundary(1) // {}", d, lowBoundary);
    log.debug("{}.highBoundary(1) // {}", d, highBoundary);
    assertEquals(new BigDecimal("-0.004"), lowBoundary);
    assertEquals(new BigDecimal("-0.003"), highBoundary);
  }

}
