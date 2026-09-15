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

package au.csiro.pathling.fhirpath;

import au.csiro.pathling.definition.FhirType;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.CodingCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.DateCollection;
import au.csiro.pathling.fhirpath.collection.DateTimeCollection;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.QuantityCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.collection.TimeCollection;
import au.csiro.pathling.fhirpath.encoding.CodingSchema;
import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * Represents one of the types defined within the FHIRPath specification.
 *
 * @author John Grimes
 */
@Getter
public enum FhirPathType {

  /** Boolean FHIRPath type. */
  BOOLEAN("Boolean", DataTypes.BooleanType, BooleanCollection.class, FhirType.BOOLEAN),

  /** String FHIRPath type. */
  STRING("String", DataTypes.StringType, StringCollection.class, FhirType.STRING),

  /** Integer FHIRPath type. */
  INTEGER("Integer", DataTypes.IntegerType, IntegerCollection.class, FhirType.INTEGER),

  /** Decimal FHIRPath type. */
  DECIMAL("Decimal", DecimalCollection.getDecimalType(), DecimalCollection.class, FhirType.DECIMAL),

  /** Date FHIRPath type. */
  DATE("Date", DataTypes.StringType, DateCollection.class, FhirType.DATE),

  /** DateTime FHIRPath type. */
  DATETIME("DateTime", DataTypes.StringType, DateTimeCollection.class, FhirType.DATETIME),

  /** Time FHIRPath type. */
  TIME("Time", DataTypes.StringType, TimeCollection.class, FhirType.TIME),

  /** Coding FHIRPath type. */
  CODING("Coding", CodingSchema.codingStructType(), CodingCollection.class, FhirType.CODING),

  /** Quantity FHIRPath type. */
  QUANTITY("Quantity", QuantityEncoding.dataType(), QuantityCollection.class, FhirType.QUANTITY),

  /** Nothing FHIRPath type (empty collection). */
  NOTHING("Nothing", DataTypes.NullType, EmptyCollection.class, FhirType.NULL);

  @Nonnull private final String typeSpecifier;

  @Nonnull private final DataType sqlDataType;

  @Nonnull private final Class<? extends Collection> collectionClass;

  @Nonnull private final FhirType defaultFhirType;

  // Maps FHIR types to FhirPathType
  @Nonnull
  private static final Map<FhirType, FhirPathType> FHIR_TYPE_TO_FHIR_PATH_TYPE =
      new ImmutableMap.Builder<FhirType, FhirPathType>()
          .put(FhirType.BOOLEAN, BOOLEAN)
          .put(FhirType.STRING, STRING)
          .put(FhirType.URI, STRING)
          .put(FhirType.URL, STRING)
          .put(FhirType.CANONICAL, STRING)
          .put(FhirType.CODE, STRING)
          .put(FhirType.OID, STRING)
          .put(FhirType.ID, STRING)
          .put(FhirType.UUID, STRING)
          .put(FhirType.MARKDOWN, STRING)
          .put(FhirType.BASE64BINARY, STRING)
          .put(FhirType.INTEGER, INTEGER)
          .put(FhirType.UNSIGNEDINT, INTEGER)
          .put(FhirType.POSITIVEINT, INTEGER)
          .put(FhirType.DECIMAL, DECIMAL)
          .put(FhirType.DATE, DATE)
          .put(FhirType.DATETIME, DATETIME)
          .put(FhirType.INSTANT, DATETIME)
          .put(FhirType.TIME, TIME)
          .put(FhirType.CODING, CODING)
          .put(FhirType.QUANTITY, QUANTITY)
          .build();

  FhirPathType(
      @Nonnull final String typeSpecifier,
      @Nonnull final DataType sqlDataType,
      @Nonnull final Class<? extends Collection> collectionClass,
      @Nonnull final FhirType defaultFhirType) {
    this.typeSpecifier = typeSpecifier;
    this.sqlDataType = sqlDataType;
    this.collectionClass = collectionClass;
    this.defaultFhirType = defaultFhirType;
  }

  /**
   * Checks if the type specifier is a valid FHIRPath type.
   *
   * @param typeSpecifier a type specifier
   * @return true if the type specifier is a valid FHIRPath type
   */
  public static boolean isValidFhirPathType(@Nonnull final String typeSpecifier) {
    for (final FhirPathType fhirPathType : FhirPathType.values()) {
      if (fhirPathType.getTypeSpecifier().equals(typeSpecifier)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Gets the FhirPathType for a given FHIR type.
   *
   * @param fhirType a {@link FhirType}
   * @return the corresponding {@link FhirPathType} according to the rules of automatic conversion
   *     within the FHIR spec
   */
  @Nonnull
  public static Optional<FhirPathType> forFhirType(@Nonnull final FhirType fhirType) {
    return Optional.ofNullable(FHIR_TYPE_TO_FHIR_PATH_TYPE.get(fhirType));
  }

  /**
   * Gets the FHIR types that correspond to this FhirPathType.
   *
   * @return a list of FHIR types that correspond to this FhirPathType
   */
  @Nonnull
  public List<FhirType> getFhirTypes() {
    // This method is currently returning an empty list
    return FHIR_TYPE_TO_FHIR_PATH_TYPE.entrySet().stream()
        .filter(entry -> entry.getValue() == this)
        .map(Map.Entry::getKey)
        .toList();
  }
}
