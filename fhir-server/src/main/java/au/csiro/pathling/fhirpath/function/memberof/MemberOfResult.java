/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.memberof;

import java.io.Serializable;
import java.util.Objects;
import lombok.Data;

/**
 * Used to represent the result of a $validate-code operation. The hash is used for correlation
 * between the input concepts and results, without needing to serialize the entire concept.
 *
 * @author John Grimes
 */
@Data
public class MemberOfResult implements Serializable {

  private static final long serialVersionUID = 4452195634956143175L;

  private int hash;
  private boolean result;

  /**
   * No arguments constructor to allow for serialization / deserialization.
   */
  public MemberOfResult() {
  }

  /**
   * @param hash The value used for correlating input concepts to results
   */
  public MemberOfResult(final int hash) {
    this.hash = hash;
  }

  /**
   * @param hash The value used for correlating input concepts to results
   * @param result The boolean result of the operation
   */
  public MemberOfResult(final int hash, final boolean result) {
    this.hash = hash;
    this.result = result;
  }

  @Override
  @SuppressWarnings("NonFinalFieldReferenceInEquals")
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final MemberOfResult that = (MemberOfResult) o;
    return hash == that.hash;
  }

  @Override
  @SuppressWarnings("NonFinalFieldReferencedInHashCode")
  public int hashCode() {
    return Objects.hash(hash);
  }

}
