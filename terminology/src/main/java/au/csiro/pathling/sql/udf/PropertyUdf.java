package au.csiro.pathling.sql.udf;

import au.csiro.pathling.encoders.datatypes.DecimalCustomCoder;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import au.csiro.pathling.terminology.TerminologyService2;
import au.csiro.pathling.terminology.TerminologyService2.Property;
import au.csiro.pathling.terminology.TerminologyService2.PropertyOrDesignation;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.model.api.annotation.DatatypeDef;
import com.google.common.collect.ImmutableSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.P;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.PrimitiveType;
import org.hl7.fhir.r4.model.Type;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static au.csiro.pathling.fhirpath.encoding.CodingEncoding.decode;
import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.isValidCoding;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType.BOOLEAN;
import static org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType.CODE;
import static org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType.CODING;
import static org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType.DATETIME;
import static org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType.DECIMAL;
import static org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType.INTEGER;
import static org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType.STRING;

/**
 * The implementation of the 'property()' udf.
 */
@Slf4j
public class PropertyUdf<T> implements SqlFunction,
    SqlFunction2<Row, String, Object[]> {

  private static final long serialVersionUID = 7605853352299165569L;
  private static final String FUNCTION_BASE_NAME = "property";

  @Nonnull
  public static final Set<FHIRDefinedType> ALLOWED_FHIR_TYPES = ImmutableSet.of(
      STRING, CODE, INTEGER, BOOLEAN, DECIMAL, DATETIME, CODING
  );

  @Nonnull
  private final FHIRDefinedType propertyType;

  @Nonnull
  private final TerminologyServiceFactory terminologyServiceFactory;

  public static final FHIRDefinedType DEFAULT_PROPERTY_TYPE = STRING;


  PropertyUdf(@Nonnull final TerminologyServiceFactory terminologyServiceFactory,
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
    switch (propertyType) {
      case STRING:
      case CODE:
      case DATETIME:
        return DataTypes.StringType;
      case INTEGER:
        return DataTypes.IntegerType;
      case BOOLEAN:
        return DataTypes.BooleanType;
      case DECIMAL:
        return DecimalCustomCoder.decimalType();
      case CODING:
        return CodingEncoding.codingStructType();
      default:
        throw new IllegalArgumentException("Cannot map FhirType: " + propertyType);
    }
  }

  // TODO: This should somehow be integrated with the encoders
  @Nonnull
  private static Object toObjectValue(@Nonnull final Type value) {
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

  @Nullable
  protected T[] doCall(@Nullable final Coding coding, @Nullable final String propertyCode) {
    if (propertyCode == null || !isValidCoding(coding)) {
      return null;
    }
    final TerminologyService2 terminologyService = terminologyServiceFactory.buildService2();
    final List<PropertyOrDesignation> result = terminologyService.lookup(
        coding, propertyCode, null);

    return (T[]) result.stream()
        .filter(s -> s instanceof Property)
        .map(s -> (Property) s)
        .filter(p -> isNull(propertyCode) || propertyCode.equals(p.getCode()))
        .map(Property::getValue)
        .filter(v -> propertyType.toCode().equals(v.fhirType()))
        .map(PropertyUdf::toObjectValue)
        .toArray(Object[]::new);
  }


  @Nullable
  private Object[] encodeArray(@Nullable Object[] objectRows) {
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
  public Object[] call(@Nullable final Row codingRow, @Nullable final String propertyCode) {
    return encodeArray(doCall(nonNull(codingRow)
                              ? decode(codingRow)
                              : null, propertyCode));
  }

  @Nonnull
  public static PropertyUdf<?> forType(
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory,
      @Nonnull final FHIRDefinedType propertyType) {
    return new PropertyUdf<>(terminologyServiceFactory, propertyType);
  }

  @Nonnull
  public static PropertyUdf<?> forClass(
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory,
      @Nonnull final Class<? extends Type> propertyClass) {
    final FHIRDefinedType propertyType = FHIRDefinedType.fromCode(
        propertyClass.getAnnotation(DatatypeDef.class).name());
    return forType(terminologyServiceFactory, propertyType);
  }

  @Nonnull
  public static String getNameForType(FHIRDefinedType propertyType) {
    if (!ALLOWED_FHIR_TYPES.contains(propertyType)) {
      throw new InvalidUserInputError(
          String.format("Type: '%s' is not supported for 'property' udf", propertyType.toCode()));
    }
    return String.format("%s_%s", FUNCTION_BASE_NAME, propertyType.getDisplay());
  }

  @Nonnull
  public static List<PropertyUdf<?>> createAll(
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    return ALLOWED_FHIR_TYPES.stream().map(t -> forType(terminologyServiceFactory, t))
        .collect(Collectors.toUnmodifiableList());
  }
}
