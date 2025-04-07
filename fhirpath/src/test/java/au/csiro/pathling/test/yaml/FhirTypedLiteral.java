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

  @Nonnull
  public static FhirTypedLiteral toQuantity(@Nonnull final String literal) {
    return of(Enumerations.FHIRDefinedType.QUANTITY, literal);
  }

  @Nonnull
  public static FhirTypedLiteral toCoding(@Nonnull final String literal) {
    return of(Enumerations.FHIRDefinedType.CODING, literal);
  }
}
