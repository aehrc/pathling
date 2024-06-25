package au.csiro.pathling.sql.boundary;

import au.csiro.pathling.sql.udf.AbstractUDFRegistrar;
import javax.annotation.Nonnull;

public class BoundaryUDFRegistrar extends AbstractUDFRegistrar {

  @Override
  protected void registerUDFs(@Nonnull final UDFRegistrar udfRegistrar) {
    udfRegistrar.register(new HighBoundaryForDateTimeFunction())
        .register(new HighBoundaryForDateFunction())
        .register(new HighBoundaryForDecimal())
        .register(new HighBoundaryForTimeFunction())
        .register(new LowBoundaryForDateTimeFunction())
        .register(new LowBoundaryForDateFunction())
        .register(new LowBoundaryForDecimal())
        .register(new LowBoundaryForTimeFunction());
  }

}
