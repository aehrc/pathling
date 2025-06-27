package au.csiro.pathling.views;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Value;

/**
 * Additional metadata describing a column.
 *
 * @see <a
 * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition.html#type-hinting-with-tags">Type
 * Hinting with Tags</a>
 */
@Data(staticConstructor = "of")
@NoArgsConstructor
public class ColumnTag {

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
