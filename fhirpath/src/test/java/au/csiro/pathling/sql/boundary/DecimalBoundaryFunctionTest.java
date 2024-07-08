package au.csiro.pathling.sql.boundary;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
  void lowBoundaryMaxPrecision() throws Exception {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, null);
    log.debug("{}.lowBoundary() // {}", d, result);
    // assertEquals(new BigDecimal("1.5870000000000000000000000000000000000"), result);
  }

  @Test
  @Order(2)
  void lowBoundaryExpandedPrecision() throws Exception {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 4);
    log.debug("{}.lowBoundary(4) // {}", d, result);
    // assertEquals(new BigDecimal("1.5870"), result);
  }

  @Test
  @Order(3)
  void lowBoundaryContractedPrecision() throws Exception {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 2);
    log.debug("{}.lowBoundary(2) // {}", d, result);
    // assertEquals(new BigDecimal("1.58"), result);
  }

  @Test
  @Order(4)
  void highBoundaryMaxPrecision() throws Exception {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, null);
    log.debug("{}.highBoundary() // {}", d, result);
    // assertEquals(new BigDecimal("1.5879999999999999999999999999999999999"), result);
  }

  @Test
  @Order(5)
  void highBoundaryExpandedPrecision() throws Exception {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, 4);
    log.debug("{}.highBoundary(4) // {}", d, result);
    // assertEquals(new BigDecimal("1.5879"), result);
  }

  @Test
  @Order(6)
  void highBoundaryContractedPrecision() throws Exception {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, 2);
    log.debug("{}.highBoundary(2) // {}", d, result);
    // assertEquals(new BigDecimal("1.59"), result);
  }

  @Test
  @Order(7)
  void lowBoundaryIntegerMaxPrecision() throws Exception {
    final BigDecimal d = new BigDecimal("1");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, null);
    log.debug("{}.lowBoundary() // {}", d, result);
    // assertEquals(new BigDecimal("1.0000000000000000000000000000000000000"), result);
  }

  @Test
  @Order(8)
  void lowBoundaryIntegerZeroPrecision() throws Exception {
    final BigDecimal d = new BigDecimal("1");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 0);
    log.debug("{}.lowBoundary(0) // {}", d, result);
    // assertEquals(new BigDecimal("1"), result);
  }

  @Test
  @Order(9)
  void lowBoundaryIntegerExpandedPrecision() throws Exception {
    final BigDecimal d = new BigDecimal("1");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 4);
    log.debug("{}.lowBoundary(4) // {}", d, result);
    // assertEquals(new BigDecimal("1.0000"), result);
  }

  @Test
  @Order(10)
  void highBoundaryIntegerMaxPrecision() throws Exception {
    final BigDecimal d = new BigDecimal("1");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, null);
    log.debug("{}.highBoundary() // {}", d, result);
    // assertEquals(new BigDecimal("1.9999999999999999999999999999999999999"), result);
  }

  @Test
  @Order(11)
  void highBoundaryIntegerZeroPrecision() throws Exception {
    final BigDecimal d = new BigDecimal("1");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, 0);
    log.debug("{}.highBoundary(0) // {}", d, result);
    assertEquals(new BigDecimal("1"), result);
  }

  @Test
  @Order(12)
  void highBoundaryIntegerExpandedPrecision() throws Exception {
    final BigDecimal d = new BigDecimal("1");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, 4);
    log.debug("{}.highBoundary(4) // {}", d, result);
    // assertEquals(new BigDecimal("1.9999"), result);
  }

  @Test
  @Order(13)
  void lowBoundaryNegativeMaxPrecision() throws Exception {
    final BigDecimal d = new BigDecimal("-1.587");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, null);
    log.debug("{}.lowBoundary() // {}", d, result);
    // assertEquals(new BigDecimal("-1.5879999999999999999999999999999999999"), result);
  }

  @Test
  @Order(14)
  void lowBoundaryNegativeExpandedPrecision() throws Exception {
    final BigDecimal d = new BigDecimal("-1.587");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 4);
    log.debug("{}.lowBoundary(4) // {}", d, result);
    // assertEquals(new BigDecimal("-1.5879"), result);
  }

  @Test
  @Order(15)
  void lowBoundaryNegativeContractedPrecision() throws Exception {
    final BigDecimal d = new BigDecimal("-1.587");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 2);
    log.debug("{}.lowBoundary(2) // {}", d, result);
    assertEquals(new BigDecimal("-1.58"), result);
  }

  @Test
  @Order(16)
  void highBoundaryNegativeMaxPrecision() throws Exception {
    final BigDecimal d = new BigDecimal("-1.587");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, null);
    log.debug("{}.highBoundary() // {}", d, result);
    // assertEquals(new BigDecimal("-1.5870000000000000000000000000000000000"), result);
  }

  @Test
  @Order(17)
  void highBoundaryNegativeExpandedPrecision() throws Exception {
    final BigDecimal d = new BigDecimal("-1.587");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, 4);
    log.debug("{}.highBoundary(4) // {}", d, result);
    // assertEquals(new BigDecimal("-1.5879"), result);
  }

  @Test
  @Order(18)
  void highBoundaryNegativeContractedPrecision() throws Exception {
    final BigDecimal d = new BigDecimal("-1.587");
    final BigDecimal result = DecimalBoundaryFunction.highBoundaryForDecimal(d, 2);
    log.debug("{}.highBoundary(2) // {}", d, result);
    // assertEquals(new BigDecimal("-1.58"), result);
  }

  @Test
  @Order(19)
  void precisionHigherThanMax() throws Exception {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, 38);
    log.debug("{}.lowBoundary(38) // \\{}", d, result);
    // assertNull(result);
  }

  @Test
  @Order(20)
  void negativePrecision() throws Exception {
    final BigDecimal d = new BigDecimal("1.587");
    final BigDecimal result = DecimalBoundaryFunction.lowBoundaryForDecimal(d, -1);
    log.debug("{}.lowBoundary(-1) // \\{}", d, result);
    // assertNull(result);
  }

}
