/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.parsing;

/**
 * Generates strings suitable for use as table aliases within generated SQL queries. Instantiate
 * once for each scope within which the aliases need to be unique.
 *
 * @author John Grimes
 */
public class AliasGenerator {

  private int index = 0;

  /**
   * Returns a String based upon the current index such that 0 = "a", 1 = "b", 26 = "aa", 27 = "ab",
   * etc. Also increments the index.
   */
  public String getAlias() {
    String result = "";
    int quotient = index, remainder;
    while (quotient >= 0) {
      remainder = quotient % 26;
      result = (char) (remainder + 97) + result;
      quotient = (int) Math.floor(quotient / 26) - 1;
    }
    index += 1;
    return result;
  }

}
