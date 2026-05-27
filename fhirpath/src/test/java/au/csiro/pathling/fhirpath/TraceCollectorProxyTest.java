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

package au.csiro.pathling.fhirpath;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.ListTraceCollector.TraceEntry;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TraceCollectorProxy}.
 *
 * @author Piotr Szul
 */
class TraceCollectorProxyTest {

  @Test
  void createReturnsNonNullProxy() {
    // Creating a proxy from a delegate should return a non-null instance.
    final ListTraceCollector delegate = new ListTraceCollector();
    try (final TraceCollectorProxy proxy = TraceCollectorProxy.create(delegate)) {
      assertNotNull(proxy);
    }
  }

  @Test
  void delegatesAddToRegisteredCollector() {
    // Calling add() on the proxy should forward the entry to the delegate.
    final ListTraceCollector delegate = new ListTraceCollector();
    try (final TraceCollectorProxy proxy = TraceCollectorProxy.create(delegate)) {
      proxy.add("lbl", "string", "val");

      final List<TraceEntry> entries = delegate.getEntries();
      assertEquals(1, entries.size());
      assertEquals("lbl", entries.get(0).label());
      assertEquals("string", entries.get(0).fhirType());
      assertEquals("val", entries.get(0).value());
    }
  }

  @Test
  void delegatesMultipleAdds() {
    // Multiple add() calls should all arrive at the delegate.
    final ListTraceCollector delegate = new ListTraceCollector();
    try (final TraceCollectorProxy proxy = TraceCollectorProxy.create(delegate)) {
      proxy.add("a", "string", "1");
      proxy.add("b", "boolean", true);
      proxy.add("c", "integer", 42);

      assertEquals(3, delegate.getEntries().size());
    }
  }

  @Test
  void addNoOpsAfterClose() {
    // After close(), add() should silently do nothing.
    final ListTraceCollector delegate = new ListTraceCollector();
    final TraceCollectorProxy proxy = TraceCollectorProxy.create(delegate);
    proxy.add("before", "string", "x");
    proxy.close();

    proxy.add("after", "string", "y");

    final List<TraceEntry> entries = delegate.getEntries();
    assertEquals(1, entries.size());
    assertEquals("before", entries.get(0).label());
  }

  @Test
  void closeIsIdempotent() {
    // Calling close() twice should not throw.
    final ListTraceCollector delegate = new ListTraceCollector();
    final TraceCollectorProxy proxy = TraceCollectorProxy.create(delegate);
    proxy.close();
    assertDoesNotThrow(proxy::close);
  }

  @Test
  void multipleProxiesAreIndependent() {
    // Two proxies with different delegates should not interfere with each other.
    final ListTraceCollector delegate1 = new ListTraceCollector();
    final ListTraceCollector delegate2 = new ListTraceCollector();

    try (final TraceCollectorProxy proxy1 = TraceCollectorProxy.create(delegate1);
        final TraceCollectorProxy proxy2 = TraceCollectorProxy.create(delegate2)) {
      proxy1.add("from1", "string", "val1");

      assertEquals(1, delegate1.getEntries().size());
      assertTrue(delegate2.getEntries().isEmpty());
    }
  }

  @Test
  void addWithNullValueDelegates() {
    // A null value should be forwarded to the delegate without error.
    final ListTraceCollector delegate = new ListTraceCollector();
    try (final TraceCollectorProxy proxy = TraceCollectorProxy.create(delegate)) {
      proxy.add("lbl", "string", null);

      assertEquals(1, delegate.getEntries().size());
      assertNull(delegate.getEntries().get(0).value());
    }
  }
}
