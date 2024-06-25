package au.csiro.pathling.sql.boundary;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class DecimalBoundaryFunctionTest {

  @Test
  void lowBoundaryMaxPrecision() {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, null);
    log.debug("{}.lowBoundary() // {}", d, result);
    assertEquals(new BigDecimal("1.5869999999999999999999999999999999995"), result);
  }

  @Test
  void lowBoundaryExpandedPrecision() {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 4);
    log.debug("{}.lowBoundary(4) // {}", d, result);
    assertEquals(new BigDecimal("1.58695"), result);
  }

  @Test
  void lowBoundaryContractedPrecision() {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 2);
    log.debug("{}.lowBoundary(2) // {}", d, result);
    assertEquals(new BigDecimal("1.582"), result);
  }

  @Test
  void highBoundaryMaxPrecision() {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, null);
    log.debug("{}.highBoundary() // {}", d, result);
    assertEquals(new BigDecimal("1.5870000000000000000000000000000000005"), result);
  }

  @Test
  void highBoundaryExpandedPrecision() {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, 4);
    log.debug("{}.highBoundary(4) // {}", d, result);
    assertEquals(new BigDecimal("1.58705"), result);
  }

  @Test
  void highBoundaryContractedPrecision() {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, 2);
    log.debug("{}.highBoundary(2) // {}", d, result);
    assertEquals(new BigDecimal("1.592"), result);
  }

  @Test
  void lowBoundaryIntegerMaxPrecision() {
    final BigDecimal d = new BigDecimal("1");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, null);
    log.debug("{}.lowBoundary() // {}", d, result);
    assertEquals(new BigDecimal("0.9999999999999999999999999999999999995"), result);
  }

  @Test
  void lowBoundaryIntegerZeroPrecision() {
    final BigDecimal d = new BigDecimal("1");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 0);
    log.debug("{}.lowBoundary(0) // {}", d, result);
    assertEquals(new BigDecimal("0.5"), result);
  }

  @Test
  void lowBoundaryIntegerExpandedPrecision() {
    final BigDecimal d = new BigDecimal("1");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 4);
    log.debug("{}.lowBoundary(4) // {}", d, result);
    assertEquals(new BigDecimal("0.99995"), result);
  }

  @Test
  void highBoundaryIntegerMaxPrecision() {
    final BigDecimal d = new BigDecimal("1");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, null);
    log.debug("{}.highBoundary() // {}", d, result);
    assertEquals(new BigDecimal("1.0000000000000000000000000000000000005"), result);
  }

  @Test
  void highBoundaryIntegerZeroPrecision() {
    final BigDecimal d = new BigDecimal("1");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, 0);
    log.debug("{}.highBoundary(0) // {}", d, result);
    assertEquals(new BigDecimal("1.5"), result);
  }

  @Test
  void highBoundaryIntegerExpandedPrecision() {
    final BigDecimal d = new BigDecimal("1");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, 4);
    log.debug("{}.highBoundary(4) // {}", d, result);
    assertEquals(new BigDecimal("1.00005"), result);
  }

  @Test
  void precisionHigherThanMax() {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 37);
    log.debug("{}.lowBoundary(34) // \\{}", d, result);
    assertNull(result);
  }

  @Test
  void negativePrecision() {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, -1);
    log.debug("{}.lowBoundary(-1) // \\{}", d, result);
    assertNull(result);
  }

}
