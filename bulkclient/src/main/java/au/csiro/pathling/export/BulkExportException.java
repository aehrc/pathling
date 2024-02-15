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

import au.csiro.pathling.export.fhir.OperationOutcome;
import au.csiro.pathling.export.ws.RetryValue;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.http.HttpResponse;

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

  @Getter
  public static class HttpError extends BulkExportException {

    private static final long serialVersionUID = -2870584282639223558L;
    final int statusCode;

    @Nonnull
    final Optional<OperationOutcome> operationOutcome;

    @Nonnull
    final Optional<RetryValue> retryAfter;

    public HttpError(@Nonnull final String message, final int statusCode,
        @Nonnull final Optional<OperationOutcome> operationOutcome,
        @Nonnull final Optional<RetryValue> retryAfter) {
      super(toDetailedMessage(message, statusCode, operationOutcome, retryAfter));
      this.statusCode = statusCode;
      this.operationOutcome = operationOutcome;
      this.retryAfter = retryAfter;
    }

    public HttpError(@Nonnull final String message, final int statusCode) {
      this(message, statusCode, Optional.empty(), Optional.empty());
    }

    public boolean isTransient() {
      return operationOutcome.map(OperationOutcome::isTransient)
          .orElse(false);
    }

    @Nonnull
    public static HttpError of(@Nonnull final String message,
        @Nonnull final HttpResponse response) {
      return new HttpError(message, response.getStatusLine().getStatusCode(), Optional.empty(),
          Optional.empty());
    }


    @Nonnull
    private static String toDetailedMessage(@Nonnull final String message, final int statusCode,
        @Nonnull final Optional<OperationOutcome> operationOutcome,
        @Nonnull final Optional<RetryValue> retryAfter) {

      final String details = Stream.of(
              Optional.of("statusCode: " + statusCode),
              operationOutcome.map(o -> "operationOutcome: " + o),
              retryAfter.map(o -> "retryAfter: " + o))
          .flatMap(Optional::stream)
          .collect(Collectors.joining(", "));

      return String.format("%s: [%s]", message, details);
    }
  }


  public static class DownloadError extends BulkExportException {

    private static final long serialVersionUID = 2425985144670724776L;

    public DownloadError(@Nonnull final String message, final Throwable cause) {
      super(message, cause);
    }
  }

  public static class ProtocolError extends BulkExportException {

    private static final long serialVersionUID = -65793456918228699L;

    public ProtocolError(@Nonnull final String message) {
      super(message);
    }
  }
}
