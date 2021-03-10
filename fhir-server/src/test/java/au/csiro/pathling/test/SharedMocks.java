/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import org.mockito.Mockito;

public class SharedMocks {

  private final static Map<Class<?>, Object> MOCKS = new HashMap<>();

  @Nonnull
  public static <T> T getOrCreate(@Nonnull final Class<T> clazz) {
    synchronized (MOCKS) {
      //noinspection unchecked
      return (T) MOCKS.computeIfAbsent(clazz, Mockito::mock);
    }
  }
}
