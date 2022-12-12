package au.csiro.pathling.fhirpath;

import org.hl7.fhir.r4.model.Coding;
import javax.annotation.Nullable;

/**
 * Helper functions for codings.
 */
public interface CodingHelpers {

  /**
   * Tests for coding equality using only their systems, codes and versions. Two codings are equal
   * if both their systems and codes are equal and either the versions are equal or at least one
   * version is null.
   *
   * @param left the left coding.
   * @param right the right coding.
   * @return if codings satisfy the equality criteria above.
   */
  static boolean codingEquals(@Nullable final Coding left,
      @Nullable final Coding right) {
    if (left == null) {
      return right == null;
    } else {

      // TODO: reconsider for immutable systems (ignore version all together)
      return right != null &&
          (left.hasSystem()
           ? left.getSystem().equals(right.getSystem())
           : !right.hasSystem()) &&
          (left.hasCode()
           ? left.getCode().equals(right.getCode())
           : !right.hasCode()) &&
          (!left.hasVersion() || !right.hasVersion() || left.getVersion()
              .equals(right.getVersion()));
    }
  }
}
