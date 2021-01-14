/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import java.util.Arrays;
import javax.annotation.Nullable;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * @author John Grimes
 */
public class DefaultAnswer implements Answer {

  @Override
  public Object answer(@Nullable final InvocationOnMock invocation) {
    checkNotNull(invocation);
    throw new AssertionError(String.format("Calling a mock with undefined arguments: %s %s",
        invocation.getMethod(),
        Arrays.toString(invocation.getArguments())));
  }

}
