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

package au.csiro.pathling.operations.bulkimport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import au.csiro.filestore.FileStore;
import au.csiro.filestore.FileStore.FileHandle;
import java.io.IOException;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BorrowedFileStore}.
 *
 * @author John Grimes
 */
class BorrowedFileStoreTest {

  /**
   * Closing the wrapper must not close the delegate, since the delegate holds a file system that
   * the server shares with the rest of its work. Closing it would leave the warehouse unreachable
   * until the server was restarted.
   */
  @Test
  void closeDoesNotCloseTheDelegate() throws IOException {
    // Given
    final FileStore delegate = mock(FileStore.class);

    // When
    new BorrowedFileStore(delegate).close();

    // Then
    verify(delegate, never()).close();
  }

  /** File access is passed straight through to the delegate. */
  @Test
  void getDelegatesToTheWrappedStore() {
    // Given
    final FileStore delegate = mock(FileStore.class);
    final FileHandle handle = mock(FileHandle.class);
    when(delegate.get("some/location")).thenReturn(handle);

    // When
    final FileHandle result = new BorrowedFileStore(delegate).get("some/location");

    // Then
    assertThat(result).isSameAs(handle);
    verify(delegate).get("some/location");
  }
}
