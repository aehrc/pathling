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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.ListTraceCollector.TraceEntry;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for {@link ListTraceCollector}. */
class ListTraceCollectorTest {

  @Test
  void emptyByDefault() {
    final ListTraceCollector collector = new ListTraceCollector();
    assertTrue(collector.getEntries().isEmpty());
  }

  @Test
  void capturesEntries() {
    final ListTraceCollector collector = new ListTraceCollector();
    collector.add("label1", "string", "hello");
    collector.add("label2", "boolean", true);

    final List<TraceEntry> entries = collector.getEntries();
    assertEquals(2, entries.size());
    assertEquals("label1", entries.get(0).label());
    assertEquals("string", entries.get(0).fhirType());
    assertEquals("hello", entries.get(0).value());
    assertEquals("label2", entries.get(1).label());
    assertEquals("boolean", entries.get(1).fhirType());
    assertEquals(true, entries.get(1).value());
  }

  @Test
  void acceptsNullValues() {
    final ListTraceCollector collector = new ListTraceCollector();
    collector.add("label", "string", null);

    assertEquals(1, collector.getEntries().size());
    assertEquals(null, collector.getEntries().get(0).value());
  }
}
