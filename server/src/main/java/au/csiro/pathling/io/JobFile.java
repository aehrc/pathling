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

package au.csiro.pathling.io;

import jakarta.annotation.Nonnull;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

/**
 * Represents a file successfully resolved within a per-job directory in the warehouse, opened for
 * reading via the Hadoop {@link org.apache.hadoop.fs.FileSystem} API.
 *
 * <p>The caller owns the {@link FSDataInputStream} and is responsible for closing it.
 *
 * @author John Grimes
 */
@Getter
@RequiredArgsConstructor
public final class JobFile {

  /** The qualified Hadoop path of the file. */
  @Nonnull private final Path path;

  /** The opened input stream. */
  @Nonnull private final FSDataInputStream stream;

  /** The size of the file in bytes. */
  private final long length;
}
