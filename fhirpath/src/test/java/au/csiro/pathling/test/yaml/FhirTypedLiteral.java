package au.csiro.pathling.test.yaml;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations;

@JsonSerialize(using = YamlSupport.FhirTypedLiteralSerializer.class)
@Value(staticConstructor = "of")
public class FhirTypedLiteral {

  @Nonnull
  Enumerations.FHIRDefinedType type;
  @Nullable
  String literal;

  @Nonnull
  public String getTag() {
    return FhirTypedLiteral.toTag(type);
  }

  @Nonnull
  public static String toTag(Enumerations.FHIRDefinedType type) {
    return "!fhir." + type.toCode();
  }

  public boolean isNull() {
    return literal == null;
  }

  @Nonnull
  public static FhirTypedLiteral toQuantity(@Nullable final String literal) {
    return of(Enumerations.FHIRDefinedType.QUANTITY, literal);
  }

  @Nonnull
  public static FhirTypedLiteral toCoding(@Nullable final String literal) {
    return of(Enumerations.FHIRDefinedType.CODING, literal);
  }

  @Nonnull
  public static FhirTypedLiteral toDateTime(@Nullable final String literal) {
    return of(Enumerations.FHIRDefinedType.DATETIME, literal);
  }

  @Nonnull
  public static FhirTypedLiteral toDate(@Nullable final String literal) {
    return of(Enumerations.FHIRDefinedType.DATE, literal);
  }

  @Nonnull
  public static FhirTypedLiteral toTime(@Nullable final String literal) {
    return of(Enumerations.FHIRDefinedType.TIME, literal);
  }

  /**
   * Creates a String typed literal.
   *
   * @param literal The String value
   * @return A new FhirTypedLiteral
   */
  @Nonnull
  public static FhirTypedLiteral toString(@Nullable final String literal) {
    return of(Enumerations.FHIRDefinedType.STRING, literal);
  }

  /**
   * Creates an Integer typed literal.
   *
   * @param literal The Integer value as a string
   * @return A new FhirTypedLiteral
   */
  @Nonnull
  public static FhirTypedLiteral toInteger(@Nullable final String literal) {
    return of(Enumerations.FHIRDefinedType.INTEGER, literal);
  }

  /**
   * Creates a Decimal typed literal.
   *
   * @param literal The Decimal value as a string
   * @return A new FhirTypedLiteral
   */
  @Nonnull
  public static FhirTypedLiteral toDecimal(@Nullable final String literal) {
    return of(Enumerations.FHIRDefinedType.DECIMAL, literal);
  }

  /**
   * Creates a Boolean typed literal.
   *
   * @param literal The Boolean value as a string
   * @return A new FhirTypedLiteral
   */
  @Nonnull
  public static FhirTypedLiteral toBoolean(@Nullable final String literal) {
    return of(Enumerations.FHIRDefinedType.BOOLEAN, literal);
  }

  /**
   * Creates a null typed literal.
   *
   * @return A new FhirTypedLiteral with NULL type
   */
  @Nonnull
  public static FhirTypedLiteral toNull() {
    return of(Enumerations.FHIRDefinedType.NULL, null);
  }
}
