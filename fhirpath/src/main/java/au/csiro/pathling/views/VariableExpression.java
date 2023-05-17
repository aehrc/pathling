package au.csiro.pathling.views;

import javax.annotation.Nonnull;
import lombok.Getter;

@Getter
public class VariableExpression extends NamedExpression {

  private final WhenMany whenMany;

  public VariableExpression(@Nonnull final String expression, @Nonnull final String name,
      final WhenMany whenMany) {
    super(expression, name);
    this.whenMany = whenMany;
  }

}
