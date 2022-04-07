/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.terminology.ucum;

import au.csiro.pathling.fhirpath.element.DecimalPath;
import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import au.csiro.pathling.fhirpath.literal.QuantityLiteralPath;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.fhir.ucum.Decimal;
import org.fhir.ucum.Pair;
import org.fhir.ucum.UcumService;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Quantity.QuantityComparator;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ComparableQuantity implements UDF1<Row, Row> {

  @Nonnull
  private final UcumService ucumService;

  private static final long serialVersionUID = 2317610455653365964L;
  private static final Map<String, String> CALENDAR_DURATION_TO_UCUM = new ImmutableMap.Builder<String, String>()
      .put("second", "s")
      .put("seconds", "s")
      .put("millisecond", "ms")
      .put("milliseconds", "ms")
      .build();

  /**
   * The name of this function when used within SQL.
   */
  public static final String FUNCTION_NAME = "comparable_quantity";

  public ComparableQuantity(@Nonnull final UcumService ucumService) {
    this.ucumService = ucumService;
  }

  @Nullable
  @Override
  public Row call(@Nullable final Row row) throws Exception {
    if (row == null) {
      return null;
    }

    final Quantity input = QuantityEncoding.decode(row);

    // If system and code are not populated, the Quantity will not be comparable.
    if (!input.hasValue() || input.getSystem() == null || input.getCode() == null) {
      return null;
    }

    // If the Quantity has a comparator, it will not be comparable.
    if (input.getComparator() != null && input.getComparator() != QuantityComparator.NULL) {
      return null;
    }

    final String resolvedCode;
    if (input.getSystem().equals(Ucum.SYSTEM_URI)) {
      resolvedCode = input.getCode();
    } else if (input.getSystem().equals(QuantityLiteralPath.FHIRPATH_CALENDAR_DURATION_URI) &&
        CALENDAR_DURATION_TO_UCUM.containsKey(input.getCode())) {
      // If it is a comparable calendar duration, convert it to UCUM.
      resolvedCode = CALENDAR_DURATION_TO_UCUM.get(input.getCode());
    } else {
      // If the Quantity is not UCUM or a comparable calendar duration, it is not comparable.
      return null;
    }

    // Use the UCUM library to get the canonical form of the Quantity.
    final int maxPrecision = DecimalPath.getDecimalType().precision();
    final Decimal value = new Decimal(input.getValue().toPlainString(), maxPrecision);
    final Pair canonical = ucumService.getCanonicalForm(new Pair(value, resolvedCode));

    // Create a new Quantity object with the canonicalized result.
    final Quantity result = new Quantity();
    result.setValue(new BigDecimal(canonical.getValue().asDecimal()));
    result.setUnit(canonical.getCode());
    result.setSystem(Ucum.SYSTEM_URI);
    result.setCode(canonical.getCode());

    return QuantityEncoding.encode(result);
  }

}
