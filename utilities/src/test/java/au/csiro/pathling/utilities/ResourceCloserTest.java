/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.utilities;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.Closeable;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class ResourceCloserTest {

  @Test
  void testEmptyListOfResourceIsNoop() {
    assertDoesNotThrow(() -> new ResourceCloser().close());
  }

  @Test
  void testClosesAllRegisteredResources() throws IOException {
    final Closeable resource1 = mock(Closeable.class);
    final Closeable resource2 = mock(Closeable.class);
    final Closeable resource3 = mock(Closeable.class);

    doThrow(new IOException("Test exception")).when(resource2).close();
    
    final ResourceCloser resourceCloser = new ResourceCloser(resource1, resource2);
    assertEquals(resource3, resourceCloser.registerResource(resource3));
    resourceCloser.close();
    verify(resource1, times(1)).close();
    verify(resource2, times(1)).close();
    verify(resource3, times(1)).close();
    verifyNoMoreInteractions(resource1, resource2, resource3);
  }
}
