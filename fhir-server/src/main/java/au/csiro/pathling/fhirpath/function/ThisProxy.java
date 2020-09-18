/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import au.csiro.pathling.fhirpath.FhirPath;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * This is a proxy that wraps any type of {@link FhirPath} and intercepts method calls to make it
 * behave like a {@code $this} keyword. The {@code $this} keyword is used to access the current item
 * in the collection in functions such as {@code where}.
 *
 * @author John Grimes
 * @see <a href="http://hl7.org/fhirpath/2018Sep/index.html#functions-2">Functions</a>
 */
public class ThisProxy implements InvocationHandler {

  private static final String EXPRESSION = "$this";

  @Override
  public Object invoke(final Object proxy, final Method method, final Object[] args)
      throws Throwable {
    switch (method.getName()) {
      case "getExpression":
        return EXPRESSION;
      case "isSingular":
        // The `$this` expression is always singular, as it represents a single item from a
        // collection.
        return true;
      default:
        return method.invoke(proxy, args);
    }
  }

}

