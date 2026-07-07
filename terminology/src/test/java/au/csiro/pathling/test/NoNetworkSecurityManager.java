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
import java.security.Permission;

/**
 * A security manager used only in tests to prove that a block of code makes no external network
 * connections. Every permission other than a network connection is allowed; an outbound connection
 * to any non-loopback host raises a {@link SecurityException}. Loopback connections are permitted
 * so that Spark's own local-mode inter-thread communication is not blocked, and name resolution (a
 * connect check with a negative port) is permitted.
 *
 * @author John Grimes
 */
@SuppressWarnings({"deprecation", "removal"})
public class NoNetworkSecurityManager extends SecurityManager {

  @Override
  public void checkPermission(final Permission perm) {
    // Allow every permission; only network connections are policed, below.
  }

  @Override
  public void checkPermission(final Permission perm, final Object context) {
    // Allow every permission; only network connections are policed, below.
  }

  @Override
  public void checkConnect(final String host, final int port) {
    assertLoopback(host, port);
  }

  @Override
  public void checkConnect(final String host, final int port, final Object context) {
    assertLoopback(host, port);
  }

  private void assertLoopback(@Nullable final String host, final int port) {
    if (port < 0) {
      // A negative port denotes name resolution rather than an actual connection.
      return;
    }
    if (!isLoopback(host)) {
      throw new SecurityException(
          "Blocked an external network connection to "
              + host
              + ":"
              + port
              + " in a test that asserts no network access is performed.");
    }
  }

  /**
   * Returns whether a host refers to the loopback interface.
   *
   * @param host the host name or address
   * @return true if the host is a loopback address or is unspecified
   */
  static boolean isLoopback(@Nullable final String host) {
    if (host == null) {
      return true;
    }
    return host.equalsIgnoreCase("localhost")
        || host.startsWith("127.")
        || host.equals("::1")
        || host.equals("0:0:0:0:0:0:0:1")
        || host.equals("[::1]");
  }
}
