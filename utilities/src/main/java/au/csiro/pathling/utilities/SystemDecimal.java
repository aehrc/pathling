/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.utilities;

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;

/**
 * The Spark type of the engine's System.Decimal, defined once for the engine, which computes with
 * it, for the decimal values of terminology properties, which the engine computes with, and for the
 * numeric annotation of a decimal, which is stored at it, so that reading the annotation gives
 * exactly what decoding the text gives.
 *
 * <p>It is the type the Parquet on FHIR specification gives the {@code numeric} annotation, so that
 * a stored annotation is read without a conversion. The previous layout's encoder stores decimals
 * at {@code DECIMAL(32,6)}, which it defines for itself, because that layout's data does not change
 * with this type.
 *
 * <p>FHIR follows the rules for {@code xs:decimal} in XML Schema, under which a minimally
 * conforming processor supports at least 18 digits and may set a documented maximum of its own.
 * Large and highly precise values are rare in medicine. The most precise is a location's
 * coordinates, for which six fractional digits locate to within 10 cm. The type therefore holds six
 * fractional digits and 32 integer digits, as {@code DECIMAL(38,6)}. FHIRPath asks for at least
 * eight fractional digits, which is a recorded deviation.
 */
public final class SystemDecimal {

  /** The total number of digits the type holds. */
  public static final int PRECISION = 38;

  /** The number of fractional digits the type holds. */
  public static final int SCALE = 6;

  /** The type, {@code DECIMAL(38,6)}. */
  @Nonnull public static final DecimalType TYPE = DataTypes.createDecimalType(PRECISION, SCALE);

  private SystemDecimal() {}
}
