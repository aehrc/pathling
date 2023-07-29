package au.csiro.pathling.views;

import javax.validation.constraints.NotNull;
import lombok.Data;

/**
 * An optional list of constants that can be used in any FHIRPath expression in the view definition.
 *  These are effectively strings or numbers that can be injected into FHIRPath expressions below by
 * having {@code %constantName`} in  the expression.
 *
 * @author John Grimes
 */
@Data
public class ConstantDeclaration {

  /**
   * The name of the variable that can be referenced by following variables or columns, where users
   * would use the {@code %variable_name} syntax.
   */
  @NotNull
  String name;

  /**
   * The value of the constant name to be used in expressions below.
   */
  @NotNull
  String value;

}
