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

/**
 * Exception thrown when an error occurs during a bulk export operation.
 */
public class BulkExportException extends RuntimeException {

  private static final long serialVersionUID = 7980275150009884077L;

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message.
   */
  public BulkExportException(@Nonnull final String message) {
    super(message);
  }

  /**
   * Constructs a new exception with the specified detail message and cause.
   *
   * @param message the detail message.
   * @param cause the cause.
   */
  public BulkExportException(@Nonnull final String message, final Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a new exception with the specified cause.
   *
   * @param cause the cause.
   */
  public BulkExportException(final Throwable cause) {
    super(cause);
  }


  /**
   * Exception thrown when a timeout occurs during a bulk export operation.
   */
  public static class Timeout extends BulkExportException {

    private static final long serialVersionUID = 2425985144670724776L;

    /**
     * Constructs a new exception with the specified detail message.
     */
    public Timeout(@Nonnull final String message) {
      super(message);
    }
  }

  /**
   * Exception thrown when a system error occurs during a bulk export operation.
   */
  public static class SystemError extends BulkExportException {

    private static final long serialVersionUID = 2425985144670724776L;

    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param message the detail message.
     * @param cause the cause.
     */
    public SystemError(@Nonnull final String message, final Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * Exception thrown when an HTTP error occurs during a bulk export operation.
   */
  @Getter
  public static class HttpError extends BulkExportException {

    private static final long serialVersionUID = -2870584282639223558L;
    /**
     * The HTTP status code.
     */
    final int statusCode;

    /**
     * The optional operation outcome.
     */
    @Nonnull
    final Optional<OperationOutcome> operationOutcome;

    /**
     * The optional retry value.
     */
    @Nonnull
    final Optional<RetryValue> retryAfter;

    /**
     * Constructs a new exception with the specified detail message and http error info.
     *
     * @param message the detail message.
     * @param statusCode the HTTP status code.
     * @param operationOutcome the optional operation outcome.
     * @param retryAfter the optional retry value.
     */
    public HttpError(@Nonnull final String message, final int statusCode,
        @Nonnull final Optional<OperationOutcome> operationOutcome,
        @Nonnull final Optional<RetryValue> retryAfter) {
      super(toDetailedMessage(message, statusCode, operationOutcome, retryAfter));
      this.statusCode = statusCode;
      this.operationOutcome = operationOutcome;
      this.retryAfter = retryAfter;
    }

    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param message the detail message.
     * @param statusCode the HTTP status code.
     */
    public HttpError(@Nonnull final String message, final int statusCode) {
      this(message, statusCode, Optional.empty(), Optional.empty());
    }

    /**
     * Returns whether the error is transient based on the operation outcome.
     *
     * @return whether the error is transient.
     */
    public boolean isTransient() {
      return operationOutcome.map(OperationOutcome::isTransient)
          .orElse(false);
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

  /**
   * Exception thrown when an error occurs during a download operation.
   */
  public static class DownloadError extends BulkExportException {

    private static final long serialVersionUID = 2425985144670724776L;

    /**
     * Constructs a new exception with the specified detail message and cause.
     *
     * @param message the detail message.
     * @param cause the cause.
     */
    public DownloadError(@Nonnull final String message, final Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * Exception thrown when unexpected behaviour occurs in async protocol interaction.
   */
  public static class ProtocolError extends BulkExportException {

    private static final long serialVersionUID = -65793456918228699L;

    /**
     * Constructs a new exception with the specified detail message
     *
     * @param message the detailed message
     */
    public ProtocolError(@Nonnull final String message) {
      super(message);
    }
  }
}
