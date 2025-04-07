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
}
