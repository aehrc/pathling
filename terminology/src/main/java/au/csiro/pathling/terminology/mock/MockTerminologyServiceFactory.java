package au.csiro.pathling.terminology.mock;

import au.csiro.pathling.terminology.ObjectHolder;
import au.csiro.pathling.terminology.ObjectHolder.SingletonHolder;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyService2;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import javax.annotation.Nonnull;

public class MockTerminologyServiceFactory implements TerminologyServiceFactory {

  private final static ObjectHolder<String, TerminologyService2> service = new SingletonHolder<>();
  private static final long serialVersionUID = 7662506865030951736L;

  @Nonnull
  @Override
  public TerminologyService buildService() {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  @Override
  public TerminologyService2 buildService2() {
    return service.getOrCreate("mock", c -> new MockTerminologyService2());
  }
}
