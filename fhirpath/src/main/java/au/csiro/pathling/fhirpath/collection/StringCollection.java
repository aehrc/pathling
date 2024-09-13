package au.csiro.pathling.fhirpath.collection;

import au.csiro.pathling.fhirpath.FhirPathType;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.jetbrains.annotations.NotNull;

public class StringCollection extends Collection {

  private StringCollection(final Column column, final Optional<FhirPathType> type) {
    super(column, type);
  }

  @NotNull
  public static Collection fromLiteral(@NotNull final String fhirPath) {
    final String unquoted = fhirPath.replaceAll("^'|'$", "");
    final String unescaped = unescapeFhirPathString(unquoted);
    return new StringCollection(functions.lit(unescaped), Optional.of(FhirPathType.STRING));
  }

  /**
   * This method implements the rules for dealing with strings in the FHIRPath specification.
   *
   * @param value the string to be unescaped
   * @return the unescaped result
   * @see <a href="https://hl7.org/fhirpath/index.html#string">String</a>
   */
  private static @NotNull String unescapeFhirPathString(@NotNull String value) {
    value = value.replaceAll("\\\\/", "/");
    value = value.replaceAll("\\\\f", "\u000C");
    value = value.replaceAll("\\\\n", "\n");
    value = value.replaceAll("\\\\r", "\r");
    value = value.replaceAll("\\\\t", "\u0009");
    value = value.replaceAll("\\\\`", "`");
    value = value.replaceAll("\\\\'", "'");
    return value.replaceAll("\\\\\\\\", "\\\\");
  }

}
