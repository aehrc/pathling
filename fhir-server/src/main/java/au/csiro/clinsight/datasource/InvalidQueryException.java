/**
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use
 * is subject to license terms and conditions.
 */
package au.csiro.clinsight.datasource;

public class InvalidQueryException extends Throwable {

  public InvalidQueryException(String message) {
    super(message);
  }

}
