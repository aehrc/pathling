package au.csiro.pathling.views;

import com.google.gson.annotations.SerializedName;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Describes the selection of a column in the output.
 *
 * @author John Grimes
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class DirectSelection extends SelectClause {

  /**
   * The name of the column produced in the output.
   * <p>
   * The name is limited to letters, numbers, or underscores and cannot start with an underscore --
   * i.e. with a regular expression of {@code ^[^_][A-Za-z0-9_]+$}. This makes it usable as table
   * names in a wide variety of databases.
   */
  @NotNull
  @Size(max = 255)
  @Pattern(regexp = "^[^_][A-Za-z0-9_]+$")
  String name;

  /**
   * The FHIRPath expression for the column's content. This may be from the resource root or from a
   * variable defined above, using a {@code %var_name.rest.of.path} structure.
   */
  @NotNull
  @SerializedName("expr")
  String expression;

  /**
   * An optional human-readable description of the column.
   */
  @SerializedName("desc")
  @Nullable
  String description;

}
