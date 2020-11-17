/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.literal;

import static au.csiro.pathling.utilities.Preconditions.check;

import au.csiro.pathling.fhirpath.FhirPath;
import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Type;

/**
 * Represents a FHIRPath Quantity literal.
 *
 * @author John Grimes
 */
@Getter
public class QuantityLiteralPath extends LiteralPath {

  private static final Pattern PATTERN = Pattern.compile("([0-9.]+) ('[^']+')");

  @SuppressWarnings("WeakerAccess")
  protected QuantityLiteralPath(@Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Type literalValue) {
    super(dataset, idColumn, literalValue);
    check(literalValue instanceof Quantity);
  }

  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param fhirPath The FHIRPath representation of the literal
   * @param context An input context that can be used to build a {@link Dataset} to represent the
   * literal
   * @return A new instance of {@link LiteralPath}
   * @throws IllegalArgumentException if the literal is malformed
   */
  @Nonnull
  public static QuantityLiteralPath fromString(@Nonnull final String fhirPath,
      @Nonnull final FhirPath context) throws IllegalArgumentException {
    final Matcher matcher = PATTERN.matcher(fhirPath);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Quantity literal has invalid format: " + fhirPath);
    }

    final String rawUnit = matcher.group(2);
    final String unit = StringLiteralPath.fromString(rawUnit, context).getLiteralValue()
        .getValueAsString();
    final Quantity quantity = new Quantity();
    quantity.setUnit(unit);

    try {
      final long value = IntegerLiteralPath.fromString(fhirPath, context)
          .getLiteralValue().getValue();
      quantity.setValue(value);
    } catch (final NumberFormatException e) {
      try {
        final BigDecimal value = DecimalLiteralPath.fromString(fhirPath, context)
            .getLiteralValue().getValue();
        quantity.setValue(value);
      } catch (final NumberFormatException ex) {
        throw new IllegalArgumentException("Quantity literal has invalid format: " + fhirPath);
      }
    }

    return new QuantityLiteralPath(context.getDataset(), context.getIdColumn(), quantity);
  }

  @Nonnull
  @Override
  public String getExpression() {
    return getLiteralValue().getValue().toPlainString() + " '" + getLiteralValue().getUnit() + "'";
  }

  @Override
  public Quantity getLiteralValue() {
    return (Quantity) literalValue;
  }

  @Nonnull
  @Override
  public Quantity getJavaValue() {
    return getLiteralValue();
  }

}
