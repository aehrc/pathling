package au.csiro.pathling.test.stubs;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.test.SharedMocks;
import javax.annotation.Nonnull;
import org.slf4j.Logger;

public class TestTerminologyClientFactory implements TerminologyClientFactory {

  public TestTerminologyClientFactory() {
  }

  @Nonnull
  @Override
  public TerminologyClient build(@Nonnull Logger logger) {
    return SharedMocks.getOrCreate(TerminologyClient.class);
  }

  @Nonnull
  @Override
  public TerminologyService buildService(@Nonnull Logger logger) {
    return SharedMocks.getOrCreate(TerminologyService.class);
  }
}