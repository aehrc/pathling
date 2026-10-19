package au.csiro.pathling.io;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.regex.Pattern;

/**
 * Spike 08 prototype (throwaway). Decision 133's datatype checks for a value given as text: the FHIR
 * R4 regex and, for the date family, a successful parse of the calendar date. The text-only types pass
 * unchecked, except that the empty string is invalid for every type. The same regexes are generated
 * as SQL for A+ and A-variant (spike_checks.py), so the candidates check the same grammar.
 */
final class Spike08Checks {

  static final String YEAR = "([0-9]([0-9]([0-9][1-9]|[1-9]0)|[1-9]00)|[1-9]000)";
  static final String TIME = "([01][0-9]|2[0-3]):[0-5][0-9]:([0-5][0-9]|60)(\\.[0-9]+)?";
  static final String ZONE = "(Z|(\\+|-)((0[0-9]|1[0-3]):[0-5][0-9]|14:00))";
  static final String DATE_RE = YEAR + "(-(0[1-9]|1[0-2])(-(0[1-9]|[1-2][0-9]|3[0-1]))?)?";
  static final String DATETIME_RE = YEAR + "(-(0[1-9]|1[0-2])(-(0[1-9]|[1-2][0-9]|3[0-1])(T" + TIME
      + ZONE + ")?)?)?";
  static final String INSTANT_RE = YEAR + "-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])T" + TIME + ZONE;
  static final String TIME_RE = TIME;
  static final String DECIMAL_RE = "-?(0|[1-9][0-9]*)(\\.[0-9]+)?([eE][+-]?[0-9]+)?";
  static final String INTEGER_RE = "-?(0|[1-9][0-9]*)";

  private static final Pattern DATE = Pattern.compile(DATE_RE);
  private static final Pattern DATETIME = Pattern.compile(DATETIME_RE);
  private static final Pattern INSTANT = Pattern.compile(INSTANT_RE);
  private static final Pattern TIME_P = Pattern.compile(TIME_RE);
  private static final Pattern DECIMAL = Pattern.compile(DECIMAL_RE);
  private static final Pattern INTEGER = Pattern.compile(INTEGER_RE);

  private Spike08Checks() {}

  /** Returns null where the text is valid for the type, else the kind of the finding. */
  static String checkText(String prim, String text) {
    if (text.isEmpty()) {
      return "empty";
    }
    switch (prim) {
      case "date":
        return DATE.matcher(text).matches() && calendar(text) ? null : "invalid";
      case "dateTime":
        return DATETIME.matcher(text).matches() && calendar(text) ? null : "invalid";
      case "instant":
        return INSTANT.matcher(text).matches() && calendar(text) ? null : "invalid";
      case "time":
        return TIME_P.matcher(text).matches() ? null : "invalid";
      case "decimal":
        return DECIMAL.matcher(text).matches() ? null : "invalid";
      case "integer":
      case "unsignedInt":
      case "positiveInt":
      case "integer64":
        return integer(prim, text) == null ? "invalid" : null;
      case "boolean":
        return "true".equals(text) || "false".equals(text) ? null : "invalid";
      case "base64Binary":
        return base64(text) == null ? "invalid" : null;
      default:
        return null;
    }
  }

  /** A full date within the text, where there is one, must exist in the calendar. */
  private static boolean calendar(String text) {
    if (text.length() < 10) {
      return true;
    }
    try {
      LocalDate.parse(text.substring(0, 10));
      return true;
    } catch (DateTimeParseException e) {
      return false;
    }
  }

  /** The stored value of an integer, or null where it is not one or is out of range. */
  static Object integer(String prim, String text) {
    if (!INTEGER.matcher(text).matches()) {
      return null;
    }
    try {
      if ("integer64".equals(prim)) {
        return Long.parseLong(text);
      }
      final long v = Long.parseLong(text);
      if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
        return null;
      }
      if ("positiveInt".equals(prim) && v < 1 || "unsignedInt".equals(prim) && v < 0) {
        return null;
      }
      return (int) v;
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /** The bytes of a base64 value, whitespace allowed, or null where it is not base64. */
  static byte[] base64(String text) {
    try {
      return Base64.getMimeDecoder().decode(text.replaceAll("\\s", "").isEmpty() ? "x" : strict(text));
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private static String strict(String text) {
    final String compact = text.replaceAll("\\s", "");
    if (!compact.matches("[A-Za-z0-9+/]*={0,2}") || compact.length() % 4 != 0) {
      throw new IllegalArgumentException("not base64");
    }
    return compact;
  }
}
