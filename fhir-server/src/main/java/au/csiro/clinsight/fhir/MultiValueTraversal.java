/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

import java.util.List;
import javax.annotation.Nonnull;

/**
 * Used to hold information about elements upstream of a ResolvedElement which have multiple
 * cardinalities. This information is used in the construction of queries.
 *
 * @author John Grimes
 */
@SuppressWarnings("WeakerAccess")
public class MultiValueTraversal {

  @Nonnull
  private String path;

  @Nonnull
  private List<String> children;

  @Nonnull
  private String typeCode;

  public MultiValueTraversal(@Nonnull String path, @Nonnull List<String> children,
      @Nonnull String typeCode) {
    this.path = path;
    this.children = children;
    this.typeCode = typeCode;
  }

  @Nonnull
  public String getPath() {
    return path;
  }

  public void setPath(@Nonnull String path) {
    this.path = path;
  }

  @Nonnull
  public List<String> getChildren() {
    return children;
  }

  public void setChildren(@Nonnull List<String> children) {
    this.children = children;
  }

  @Nonnull
  public String getTypeCode() {
    return typeCode;
  }

  public void setTypeCode(@Nonnull String typeCode) {
    this.typeCode = typeCode;
  }

}
