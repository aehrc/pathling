/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
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
