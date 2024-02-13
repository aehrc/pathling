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

import org.apache.http.HttpResponse;
import java.util.Optional;
import javax.annotation.Nonnull;

public class BulkExportException extends RuntimeException {

  private static final long serialVersionUID = 7980275150009884077L;

  public BulkExportException(@Nonnull final String message) {
    super(message);
  }

  public BulkExportException(@Nonnull final String message, final Throwable cause) {
    super(message, cause);
  }
  
  public static class Timeout extends BulkExportException {

    private static final long serialVersionUID = 2425985144670724776L;

    public Timeout(@Nonnull final String message) {
      super(message);
    }
  }

  public static class HttpError extends BulkExportException {

    private static final long serialVersionUID = -2870584282639223558L;
    final int statusCode;

    @Nonnull
    final Optional<OperationOutcome> operationOutcome;

    public HttpError(@Nonnull final String message, final int statusCode,
        @Nonnull final Optional<OperationOutcome> operationOutcome) {
      super(message);
      this.statusCode = statusCode;
      this.operationOutcome = operationOutcome;
    }

    public HttpError(@Nonnull final String message, final int statusCode) {
      this(message, statusCode, Optional.empty());
    }

    public static HttpError of(@Nonnull final String message,
        @Nonnull final HttpResponse response) {
      return new HttpError(message, response.getStatusLine().getStatusCode(), Optional.empty());
    }
  }

  public static class DownloadError extends BulkExportException {

    private static final long serialVersionUID = 2425985144670724776L;

    public DownloadError(@Nonnull final String message, final Throwable cause) {
      super(message, cause);
    }
  }
}
