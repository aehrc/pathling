/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @author John Grimes
 */
public interface ExpressionFunction {

  @Nonnull
  ParseResult invoke(@Nullable ParseResult input, @Nonnull List<ParseResult> arguments);

}
