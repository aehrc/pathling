/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.test;

import static java.util.Objects.requireNonNull;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.lang.reflect.Method;
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
    getStore(requireNonNull(context)).put(START_TIME, System.currentTimeMillis());
  }

  @Override
  public void afterTestExecution(@Nullable final ExtensionContext context) {
    final Class<?> testClass = requireNonNull(context).getRequiredTestClass();
    final Method testMethod = context.getRequiredTestMethod();
    final long startTime = getStore(context).remove(START_TIME, long.class);
    final long duration = System.currentTimeMillis() - startTime;
    log.info("{}::{} took {} ms.", testClass.getSimpleName(), testMethod.getName(), duration);
  }

  private Store getStore(@Nonnull final ExtensionContext context) {
    return context.getStore(Namespace.create(getClass(), context.getRequiredTestMethod()));
  }
}
