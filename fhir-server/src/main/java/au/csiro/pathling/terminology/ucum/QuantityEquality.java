/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.terminology.ucum;

import au.csiro.pathling.fhirpath.element.DecimalPath;
import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF2;
import org.fhir.ucum.Decimal;
import org.fhir.ucum.Pair;
import org.fhir.ucum.UcumService;
import org.hl7.fhir.r4.model.Quantity;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class QuantityEquality implements UDF2<Row, Row, Boolean> {

  @Nonnull
  private final UcumService ucumService;

  private static final long serialVersionUID = -5960116379100195530L;

  /**
   * The name of this function when used within SQL.
   */
  public static final String FUNCTION_NAME = "quantity_equals";

  public QuantityEquality(@Nonnull final UcumService ucumService) {
    this.ucumService = ucumService;
  }

  @Override
  public Boolean call(final Row left, final Row right) throws Exception {
    final Quantity leftQuantity = QuantityEncoding.decode(left);
    final Quantity rightQuantity = QuantityEncoding.decode(right);

    final String leftSystem = leftQuantity.getSystem();
    final String rightSystem = rightQuantity.getSystem();
    final String leftCode = leftQuantity.getCode();
    final String rightCode = rightQuantity.getCode();
    if (!leftQuantity.hasValue() || !rightQuantity.hasValue()
        || leftSystem == null || rightSystem == null
        || leftCode == null || rightCode == null) {
      return null;
    }

    if (leftSystem.equals(rightSystem) && leftCode.equals(rightCode)) {
      return leftQuantity.getValue().equals(rightQuantity.getValue());
    }

    final int maxPrecision = DecimalPath.getDecimalType().precision();
    final Decimal leftValue = new Decimal(leftQuantity.getValue().toPlainString(), maxPrecision);
    final Decimal rightValue = new Decimal(rightQuantity.getValue().toPlainString(), maxPrecision);
    if (leftSystem.equals(Ucum.SYSTEM_URI) && rightSystem.equals(Ucum.SYSTEM_URI)
        && Objects.equals(leftQuantity.getComparator(), rightQuantity.getComparator())
        && ucumService.isComparable(leftCode, rightCode)) {
      final Pair leftCanonical = ucumService.getCanonicalForm(new Pair(leftValue, leftCode));
      final Pair rightCanonical = ucumService.getCanonicalForm(new Pair(rightValue, rightCode));
      if (!leftCanonical.getCode().equals(rightCanonical.getCode())) {
        log.warn("Encountered comparable canonical UCUM forms with different codes: {} and {}",
            leftQuantity, rightQuantity);
        return null;
      } else {
        return leftCanonical.getValue().equals(rightCanonical.getValue());
      }
    } else {
      return null;
    }
  }

}
