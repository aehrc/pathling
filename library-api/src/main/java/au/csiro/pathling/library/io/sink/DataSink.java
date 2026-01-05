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

package au.csiro.pathling.library.io.sink;

import au.csiro.pathling.io.source.DataSource;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

/**
 * Represents a data sink that knows how to read data from a {@link DataSource} and write it to some
 * form of persistence.
 *
 * @author John Grimes
 */
public interface DataSink {

  /**
   * Write the data from the source to the sink.
   *
   * @param source the data source to write
   * @return Information about the write operation or null if no details are captured.
   */
  @Nullable
  WriteDetails write(@Nonnull final DataSource source);
}
