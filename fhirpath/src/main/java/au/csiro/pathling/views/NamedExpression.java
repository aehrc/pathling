package au.csiro.pathling.views;

import javax.annotation.Nonnull;
import lombok.Getter;

@Getter
public class NamedExpression {

  @Nonnull
  private final String expression;

  @Nonnull
  private final String name;


  public NamedExpression(@Nonnull final String expression, @Nonnull final String name) {
    this.expression = expression;
    this.name = name;
  }

}
