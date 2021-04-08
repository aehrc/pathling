/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.stubs;

import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.test.SharedMocks;
import javax.annotation.Nonnull;
import org.slf4j.Logger;

public class TestTerminologyServiceFactory implements TerminologyServiceFactory {

  private static final long serialVersionUID = -8229464411116137820L;

  public TestTerminologyServiceFactory() {
  }

  @Nonnull
  @Override
  public TerminologyService buildService(@Nonnull final Logger logger) {
    return SharedMocks.getOrCreate(TerminologyService.class);
  }
}