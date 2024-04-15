package au.csiro.pathling.export.ws;

import javax.annotation.Nonnull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;


/**
 * Represents associated data for a bulk export operation.
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class AssociatedData {

  @Nonnull
  String code;

  /**
   * The latest provenance resources associated with the export operation.
   */
  public static final AssociatedData LATEST_PROVENANCE_RESOURCES = new AssociatedData(
      "LatestProvenanceResources");

  /**
   *
   */
  public static final AssociatedData RELEVANT_PROVENANCE_RESOURCES = new AssociatedData(
      "RelevantProvenanceResources");

  @Override
  @Nonnull
  public String toString() {
    return code;
  }

  /**
   * Creates a custom associated data value. The code for this value will be prefixed with an
   * underscore.
   *
   * @param customValue the custom value to create (without the underscore prefix).
   * @return the associated data.
   */
  @Nonnull
  public static AssociatedData custom(@Nonnull String customValue) {
    return new AssociatedData("_" + customValue);
  }

  /**
   * Parses an associated data value from a string. The string must be one of the known values or a
   * custom value prefixed with an underscore.
   *
   * @param code the string code to parse.
   * @return the associated data.
   * @throws IllegalArgumentException if the string code is not recognized.
   */
  @Nonnull
  public static AssociatedData fromCode(@Nonnull final String code) {
    if (LATEST_PROVENANCE_RESOURCES.code.equals(code)) {
      return LATEST_PROVENANCE_RESOURCES;
    } else if (RELEVANT_PROVENANCE_RESOURCES.code.equals(code)) {
      return RELEVANT_PROVENANCE_RESOURCES;
    } else if (code.startsWith("_")) {
      return new AssociatedData(code);
    } else {
      throw new IllegalArgumentException("Unknown associated data code: " + code);
    }
  }
}
