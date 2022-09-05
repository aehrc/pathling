/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2022, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 *
 */

package au.csiro.pathling.encoders.terminology.ucum;

import au.csiro.pathling.encoders.datatypes.DecimalCustomCoder;
import java.io.InputStream;
import java.math.BigDecimal;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.fhir.ucum.Decimal;
import org.fhir.ucum.Pair;
import org.fhir.ucum.UcumEssenceService;
import org.fhir.ucum.UcumException;
import org.fhir.ucum.UcumService;

/**
 * Makes UCUM services available to the rest of the application.
 *
 * @author John Grimes
 */
public class Ucum {

  public static final String SYSTEM_URI = "http://unitsofmeasure.org";

  private static final UcumService service;

  static {
    final InputStream essenceStream = Ucum.class.getClassLoader()
        .getResourceAsStream("tx/ucum-essence.xml");
    try {
      service = new UcumEssenceService(essenceStream);
    } catch (final UcumException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  public static UcumService service() throws UcumException {
    return service;
  }

  @Nullable
  public static BigDecimal getCanonicalValue(@Nonnull final BigDecimal value,
      @Nonnull final String code) {
    try {
      @Nullable final Pair result = getCanonicalForm(value, code);
      if (result == null) {
        return null;
      }
      @Nullable final Decimal decimalValue = result.getValue();
      if (decimalValue == null) {
        return null;
      }
      @Nullable final String stringValue = decimalValue.asDecimal();
      if (stringValue == null) {
        return null;
      }
      return new BigDecimal(stringValue);
    } catch (final UcumException e) {
      return null;
    }
  }

  @Nullable
  public static String getCanonicalCode(@Nonnull final BigDecimal value,
      @Nonnull final String code) {
    try {
      @Nullable final Pair result = getCanonicalForm(value, code);
      if (result == null) {
        return null;
      }
      return result.getCode();
    } catch (final UcumException e) {
      return null;
    }
  }

  @Nullable
  private static Pair getCanonicalForm(final @Nonnull BigDecimal value, final @Nonnull String code)
      throws UcumException {
    final int maxPrecision = DecimalCustomCoder.decimalType().precision();
    final Decimal decimalValue = new Decimal(value.toPlainString(), maxPrecision);
    @Nullable final Pair result = service.getCanonicalForm(new Pair(decimalValue, code));
    return result;
  }

}