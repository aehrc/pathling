package au.csiro.pathling.sql.boundary;

import au.csiro.pathling.sql.udf.AbstractUDFRegistrar;
import javax.annotation.Nonnull;

public class BoundaryUDFRegistrar extends AbstractUDFRegistrar {

  @Override
  protected void registerUDFs(@Nonnull final UDFRegistrar udfRegistrar) {
    udfRegistrar.register(new HighBoundaryForDateTimeFunction())
        .register(new HighBoundaryForDecimal())
        .register(new HighBoundaryForTime())
        .register(new LowBoundaryForDateTimeFunction())
        .register(new LowBoundaryForDecimal())
        .register(new LowBoundaryForTime());
  }

}
