/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.sql.udf;

import static au.csiro.pathling.fhirpath.encoding.CodingEncoding.decode;
import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.isValidCoding;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType.BOOLEAN;
import static org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType.CODE;
import static org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType.CODING;
import static org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType.DATETIME;
import static org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType.DECIMAL;
import static org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType.INTEGER;
import static org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType.STRING;

import au.csiro.pathling.encoders.datatypes.DecimalCustomCoder;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyService.Property;
import au.csiro.pathling.terminology.TerminologyService.PropertyOrDesignation;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.model.api.annotation.DatatypeDef;
import com.google.common.collect.ImmutableSet;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.Serial;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.PrimitiveType;
import org.hl7.fhir.r4.model.Type;

/**
 * The implementation of the 'property()' udf.
 */
@Slf4j
public class PropertyUdf implements SqlFunction,
    SqlFunction3<Row, String, String, Object[]> {

  @Serial
  private static final long serialVersionUID = 7605853352299165569L;
  private static final String FUNCTION_BASE_NAME = "property";

  /**
   * The set of FHIR types allowed for property values.
   */
  @Nonnull
  public static final Set<FHIRDefinedType> ALLOWED_FHIR_TYPES = ImmutableSet.of(
      STRING, CODE, INTEGER, BOOLEAN, DECIMAL, DATETIME, CODING
  );

  /**
   * The FHIR type for the property values.
   */
  @Nonnull
  private final FHIRDefinedType propertyType;

  /** The terminology service factory used to create terminology services. */
  @Nonnull
  private final TerminologyServiceFactory terminologyServiceFactory;

  /**
   * The default property FHIR type.
   */
  public static final FHIRDefinedType DEFAULT_PROPERTY_TYPE = STRING;


  /**
   * Creates a new PropertyUdf with the specified terminology service factory and property type.
   *
   * @param terminologyServiceFactory the terminology service factory to use
   * @param propertyType the FHIR type for the property values
   * @throws IllegalArgumentException if the property type is not supported
   */
  private PropertyUdf(@Nonnull final TerminologyServiceFactory terminologyServiceFactory,
      @Nonnull final FHIRDefinedType propertyType) {
    if (!ALLOWED_FHIR_TYPES.contains(propertyType)) {
      throw new IllegalArgumentException("PropertyUDF does not support type: " + propertyType);
    }
    this.propertyType = propertyType;
    this.terminologyServiceFactory = terminologyServiceFactory;
  }

  @Override
  public String getName() {
    return getNameForType(propertyType);
  }


  // TODO: This should somehow be integrated with the encoders
  @Nonnull
  private DataType geElementType() {
    // code | Coding | string | integer | boolean | dateTime | decimal
    return switch (propertyType) {
      case STRING, CODE, DATETIME -> DataTypes.StringType;
      case INTEGER -> DataTypes.IntegerType;
      case BOOLEAN -> DataTypes.BooleanType;
      case DECIMAL -> DecimalCustomCoder.decimalType();
      case CODING -> CodingEncoding.codingStructType();
      default -> throw new IllegalArgumentException("Cannot map FhirType: " + propertyType);
    };
  }

  // TODO: This should somehow be integrated with the encoders
  @Nonnull
  private static Object toObjectValue(@Nonnull final Type value) {
    // Special case for DateTimeType as we represent the as SQL strings
    if (value instanceof DateTimeType) {
      return value.primitiveValue();
    } else {
      return value instanceof PrimitiveType<?>
             ? ((PrimitiveType<?>) value).getValue()
             : value;
    }
  }

  @Override
  public DataType getReturnType() {
    return DataTypes.createArrayType(geElementType());
  }

  /**
   * Executes the property lookup operation for the given coding and property code.
   *
   * @param coding the coding to look up properties for
   * @param propertyCode the property code to retrieve
   * @param acceptLanguage the accept language header value for the request
   * @return an array of property values, or null if lookup fails
   */
  @Nullable
  protected Object[] doCall(@Nullable final Coding coding, @Nullable final String propertyCode,
      @Nullable final String acceptLanguage) {
    if (propertyCode == null || !isValidCoding(coding)) {
      return null;
    }
    final TerminologyService terminologyService = terminologyServiceFactory.build();
    final List<PropertyOrDesignation> result = terminologyService.lookup(
        requireNonNull(coding), propertyCode, acceptLanguage);

    return result.stream()
        .filter(Property.class::isInstance)
        .map(s -> (Property) s)
        .filter(p -> propertyCode.equals(p.getCode()))
        .map(Property::getValue)
        .filter(v -> propertyType.toCode().equals(v.fhirType()))
        .map(PropertyUdf::toObjectValue)
        .toArray(Object[]::new);
  }

  @Nullable
  private Object[] encodeArray(@Nullable final Object[] objectRows) {
    if (nonNull(objectRows) && CODING.equals(propertyType)) {
      return Stream.of(objectRows)
          .map(Coding.class::cast)
          .map(CodingEncoding::encode)
          .toArray(Row[]::new);
    } else {
      return objectRows;
    }
  }

  @Nullable
  @Override
  public Object[] call(@Nullable final Row codingRow, @Nullable final String propertyCode,
      @Nullable final String acceptLanguage) {
    return encodeArray(doCall(nonNull(codingRow)
                              ? decode(codingRow)
                              : null, propertyCode, acceptLanguage));
  }

  /**
   * Creates the instance of PropertyUdf for the specified FHIR type.
   *
   * @param terminologyServiceFactory the terminology service factory to use.
   * @param propertyType the type of the properties returned by this UDF.
   * @return a new instance of {@link PropertyUdf}
   */
  @Nonnull
  public static PropertyUdf forType(
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory,
      @Nonnull final FHIRDefinedType propertyType) {
    return new PropertyUdf(terminologyServiceFactory, propertyType);
  }

  /**
   * Creates the instance of PropertyUdf for the specified FHIR class.
   *
   * @param terminologyServiceFactory the terminology service factory to use.
   * @param propertyClass the class FHIR clas of the returned by this UDF.
   * @return a new instance of {@link PropertyUdf}
   */
  @Nonnull
  public static PropertyUdf forClass(
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory,
      @Nonnull final Class<? extends Type> propertyClass) {
    final FHIRDefinedType propertyType = FHIRDefinedType.fromCode(
        propertyClass.getAnnotation(DatatypeDef.class).name());
    return forType(terminologyServiceFactory, propertyType);
  }

  /**
   * Gets the name under which the UDF for given type is registered in SparkSession.
   *
   * @param propertyType the FHIR type.
   * @return the name of the UDF.
   */
  @Nonnull
  public static String getNameForType(final FHIRDefinedType propertyType) {
    if (!ALLOWED_FHIR_TYPES.contains(propertyType)) {
      throw new InvalidUserInputError(
          String.format("Type: '%s' is not supported for 'property' udf", propertyType.toCode()));
    }
    return String.format("%s_%s", FUNCTION_BASE_NAME, propertyType.getDisplay());
  }

  /**
   * Creates variants of {@link PropertyUdf} for all allowed property types.
   *
   * @param terminologyServiceFactory the terminology service factory to use.
   * @return the list of supported variants of {@link PropertyUdf}.
   */
  @Nonnull
  public static List<PropertyUdf> createAll(
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    return ALLOWED_FHIR_TYPES.stream().map(t -> forType(terminologyServiceFactory, t))
        .toList();
  }
}
