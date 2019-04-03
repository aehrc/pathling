/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import javax.annotation.Nonnull;

/**
 * This class is used in the creation of Spark Datasets that contain the results of ValueSet
 * expansions.
 *
 * @author John Grimes
 */
public class Code {

  @Nonnull
  private String system;

  @Nonnull
  private String code;

  public Code(@Nonnull String system, @Nonnull String code) {
    this.system = system;
    this.code = code;
  }

  @Nonnull
  public String getSystem() {
    return system;
  }

  public void setSystem(@Nonnull String system) {
    this.system = system;
  }

  @Nonnull
  public String getCode() {
    return code;
  }

  public void setCode(@Nonnull String code) {
    this.code = code;
  }

}
