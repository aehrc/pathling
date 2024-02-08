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

import java.net.http.HttpResponse;
import java.util.Optional;
import javax.annotation.Nonnull;

public class BulkExportException extends RuntimeException {

  public BulkExportException(String message) {
    super(message);
  }

  public BulkExportException(String message, Throwable cause) {
    super(message, cause);
  }

  public static class Timeout extends BulkExportException {

    public Timeout(String message) {
      super(message);
    }
  }

  public static class HttpError extends BulkExportException {

    final int statusCode;

    @Nonnull
    final Optional<OperationOutcome> operationOutcome;

    public HttpError(@Nonnull final String message, final int statusCode,
        @Nonnull final Optional<OperationOutcome> operationOutcome) {
      super(message);
      this.statusCode = statusCode;
      this.operationOutcome = operationOutcome;
    }

    public static HttpError of(@Nonnull final String message, HttpResponse<String> response) {
      return new HttpError(message, response.statusCode(), OperationOutcome.parse(response.body()));
    }
  }
}
