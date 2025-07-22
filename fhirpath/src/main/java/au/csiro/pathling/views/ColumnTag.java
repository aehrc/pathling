package au.csiro.pathling.views;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Additional metadata describing a column.
 *
 * @see <a
 * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition.html#type-hinting-with-tags">Type
 * Hinting with Tags</a>
 */
@Data
@AllArgsConstructor(staticName = "of")
@NoArgsConstructor
public class ColumnTag {

  /**
   * Tag for ANSI SQL type hinting.
   * <p>
   * This tag is used to provide a hint about the SQL type of the column, which can be useful for
   * database-specific optimizations or type handling.
   */
  public static final String ANSI_TYPE_TAG = "ansi/type";

  /**
   * Name of tag.
   */
  @NotNull
  String name;

  /**
   * Value of tag.
   */
  @NotNull
  String value;
}
