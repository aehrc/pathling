package au.csiro.pathling.fhirpath.encoding;

import java.io.Serializable;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.Coding;

/**
 * Immutable comparable and printable version of fhir Coding
 */

@AllArgsConstructor(staticName = "of")
@ToString
@Getter
@EqualsAndHashCode
public class ImmutableCoding implements Serializable {

  @Nullable
  private final String system;
  @Nullable
  private final String version;
  @Nullable
  private final String code;
  @Nullable
  private final String display;
  @Nullable
  protected final Boolean userSelected;

  @Nonnull
  public Coding toCoding() {
    return new Coding(system, code, display).setVersion(version).setUserSelectedElement(
        userSelected != null
        ? new BooleanType(userSelected)
        : null);
  }

  @Nonnull
  public static ImmutableCoding of(@Nonnull final Coding coding) {
    return ImmutableCoding.of(coding.getSystem(), coding.getVersion(), coding.getCode(),
        coding.getDisplay(), coding.hasUserSelected()
                             ? coding.getUserSelected()
                             : null);
  }

  @Nonnull
  public static ImmutableCoding of(@Nonnull final String system, @Nonnull final String code,
      @Nonnull final String display) {
    return ImmutableCoding.of(system, null, code, display, null);
  }


}
