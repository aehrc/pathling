/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import java.lang.reflect.Method;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;

@Slf4j
public class TimingExtension implements BeforeTestExecutionCallback, AfterTestExecutionCallback {

  private static final String START_TIME = "START_TIME";

  @Override
  public void beforeTestExecution(@Nullable final ExtensionContext context) {
    getStore(checkNotNull(context)).put(START_TIME, System.currentTimeMillis());
  }

  @Override
  public void afterTestExecution(@Nullable final ExtensionContext context) {
    final Class<?> testClass = checkNotNull(context).getRequiredTestClass();
    final Method testMethod = context.getRequiredTestMethod();
    final long startTime = getStore(context).remove(START_TIME, long.class);
    final long duration = System.currentTimeMillis() - startTime;
    log.info("{}::{} took {} ms.", testClass.getSimpleName(), testMethod.getName(), duration);
  }

  private Store getStore(@Nonnull final ExtensionContext context) {
    return context.getStore(Namespace.create(getClass(), context.getRequiredTestMethod()));
  }

}