/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.export;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.Value;

/**
 * Represents the result of a bulk export operation.
 */
@Value(staticConstructor = "of")
public class BulkExportResult {


  /**
   * Represents the result of a single file export operation.
   */
  @Value(staticConstructor = "of")
  public static class FileResult {

    /**
     * The source URI of the file that was exported.
     */
    @Nonnull
    URI source;

    /**
     * The destination URI of the file that was exported.
     */
    @Nonnull
    URI destination;

    /**
     * The size of the file that was exported in bytes.
     */
    long size;
  }

  /**
   * The time at which the transaction was processed at the server. (transactionTime from the bulk
   * export response)
   */
  @Nonnull
  Instant transactionTime;

  /**
   * The results of the export operation for individual resources.
   */
  @Nonnull
  List<FileResult> results;
}
