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

package au.csiro.pathling.export.ws;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;

/**
 * The final response for a successful bulk export request.
 *
 * @see <a href="https://hl7.org/fhir/uv/bulkdata/export.html#response---complete-status"> Response
 * - Complete Status</a>
 */
@Value
@Builder
public class BulkExportResponse implements AsyncResponse {

  /**
   * Indicates the server's time when the query is run. The 'transactionTime' response value.
   */
  @Nonnull
  Instant transactionTime;

  /**
   * The full URL of the original Bulk Data kick-off request. The 'request' response value.
   */
  @Nonnull
  String request;

  /**
   * Indicates whether downloading the generated files requires the same authorization mechanism as
   * the $export operation itself. The 'requiresAccessToken' response value.
   */
  boolean requiresAccessToken;

  /**
   * A list of file items with one entry for each generated file. The 'output' response value.
   */
  @Nonnull
  @Builder.Default
  List<FileItem> output = Collections.emptyList();

  /**
   * A list of deleted file items following the same structure as the output list.
   */
  @Nonnull
  @Builder.Default
  List<FileItem> deleted = Collections.emptyList();

  /**
   * A list of error items following the same structure as the output list.
   */
  @Nonnull
  @Builder.Default
  List<FileItem> error = Collections.emptyList();

  /**
   * Represents a single file item in the response.
   */
  @Value
  public static class FileItem {

    /**
     * The type of the FHIR resource contained in the file.
     */
    @Nonnull
    String type;

    /**
     * The URL of the file.
     */
    @Nonnull
    String url;

    /**
     * The number of resources in the file.
     */
    long count;
  }
}
