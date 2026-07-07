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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for {@link NoNetworkExtension} and {@link NoNetworkSecurityManager}: while the extension is
 * active the security manager is installed, external connections are blocked, and loopback
 * connections and name resolution are permitted.
 *
 * @author John Grimes
 */
@SuppressWarnings({"deprecation", "removal"})
@ExtendWith(NoNetworkExtension.class)
class NoNetworkExtensionTest {

  @Test
  void installsSecurityManager() {
    assertInstanceOf(NoNetworkSecurityManager.class, System.getSecurityManager());
  }

  @Test
  void blocksExternalConnection() {
    final SecurityManager securityManager = System.getSecurityManager();
    assertThrows(SecurityException.class, () -> securityManager.checkConnect("example.com", 443));
  }

  @Test
  void allowsLoopbackConnection() {
    final SecurityManager securityManager = System.getSecurityManager();
    // Spark's local-mode communication uses loopback, so it must not be blocked.
    assertDoesNotThrow(() -> securityManager.checkConnect("127.0.0.1", 7077));
  }

  @Test
  void allowsNameResolution() {
    final SecurityManager securityManager = System.getSecurityManager();
    // A negative port denotes name resolution rather than a connection.
    assertDoesNotThrow(() -> securityManager.checkConnect("example.com", -1));
  }

  @Test
  void detectsLoopbackHosts() {
    assertTrue(NoNetworkSecurityManager.isLoopback("localhost"));
    assertTrue(NoNetworkSecurityManager.isLoopback("127.0.0.1"));
    assertTrue(NoNetworkSecurityManager.isLoopback("::1"));
    assertTrue(NoNetworkSecurityManager.isLoopback(null));
    assertFalse(NoNetworkSecurityManager.isLoopback("example.com"));
    assertFalse(NoNetworkSecurityManager.isLoopback("8.8.8.8"));
  }
}
