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

import jakarta.annotation.Nullable;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * A JUnit 5 extension that installs a {@link NoNetworkSecurityManager} for the duration of each
 * test, so that any attempt to open an external (non-loopback) network connection fails the test.
 * Use it on local-mode terminology tests to prove they make no network requests.
 *
 * <p>Requires the test JVM to be started with {@code -Djava.security.manager=allow}, which the
 * project's Surefire and Failsafe configurations provide.
 *
 * @author John Grimes
 */
@SuppressWarnings({"deprecation", "removal"})
public class NoNetworkExtension implements BeforeEachCallback, AfterEachCallback {

  @Nullable private SecurityManager previous;

  @Override
  public void beforeEach(final ExtensionContext context) {
    previous = System.getSecurityManager();
    System.setSecurityManager(new NoNetworkSecurityManager());
  }

  @Override
  public void afterEach(final ExtensionContext context) {
    System.setSecurityManager(previous);
  }
}
