/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.utilities;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.LinkedList;
import javax.xml.bind.DatatypeConverter;

/**
 * Utility class containing some methods for string wrangling.
 *
 * @author John Grimes
 */
@SuppressWarnings("WeakerAccess")
public abstract class Strings {

  public static String capitalize(String name) {
    if (name == null || name.length() == 0) {
      return name;
    }
    char[] chars = name.toCharArray();
    chars[0] = Character.toUpperCase(chars[0]);
    return new String(chars);
  }

  public static String uncapitalize(String name) {
    if (name == null || name.length() == 0) {
      return name;
    }
    char[] chars = name.toCharArray();
    chars[0] = Character.toLowerCase(chars[0]);
    return new String(chars);
  }

  public static String quote(String value) {
    return "\"" + value + "\"";
  }

  public static String singleQuote(String value) {
    return "'" + value + "'";
  }

  public static String backTicks(String value) {
    return "`" + value + "`";
  }

  public static String unSingleQuote(String value) {
    return value.replaceAll("^'|'$", "");
  }

  /**
   * This method implements the rules for dealing with strings in the FHIRPath specification.
   *
   * @see <a href="http://hl7.org/fhirpath/2018Sep/index.html#string">http://hl7.org/fhirpath/2018Sep/index.html#string</a>
   */
  public static String unescapeFhirPathString(String value) {
    value = value.replaceAll("\\\\", "\\");
    value = value.replaceAll("\\/", "/");
    value = value.replaceAll("\\f", "\u000C");
    value = value.replaceAll("\\n", "\n");
    value = value.replaceAll("\\r", "\r");
    value = value.replaceAll("\\t", "\u0009");
    value = value.replaceAll("\\`", "`");
    return value.replaceAll("\\'", "'");
  }

  public static LinkedList<String> tokenizePath(String path) {
    return new LinkedList<>(Arrays.asList(path.split("\\.")));
  }

  public static String untokenizePath(Iterable<String> tokens) {
    return String.join(".", tokens);
  }

  /**
   * Calculates the MD5 hash of the input string, converts it to lowercase hex and trims it to only
   * the first 7 characters.
   */
  public static String md5Short(String input) {
    MessageDigest messageDigest;
    try {
      messageDigest = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Error calculating MD5 hash value", e);
    }
    messageDigest.update(input.getBytes());
    return DatatypeConverter.printHexBinary(messageDigest.digest()).toLowerCase().substring(0, 7);
  }

}
