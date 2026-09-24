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

package au.csiro.pathling.terminology.expand;

import jakarta.annotation.Nonnull;
import java.io.Serial;

/**
 * Thrown in SERVER mode when the first page of a canonical expansion is answered with a failure
 * status other than 404, or cannot be fetched because the terminology server cannot be reached, so
 * that whether the URL names a value set at all is unknown. Every existing handler of {@link
 * ValueSetExpansionException} still applies; a caller that distinguishes an unknown outcome from a
 * fault in a value set it has found catches this subclass first. The message carries the reason the
 * expansion built.
 *
 * @author John Grimes
 */
public class ValueSetLookupException extends ValueSetExpansionException {

  @Serial private static final long serialVersionUID = 5170948620371539034L;

  private final boolean serverUnreachable;

  /**
   * Creates an exception with the given reason and cause.
   *
   * @param reason a human-readable description of why the first page could not be fetched
   * @param cause the underlying failure
   * @param serverUnreachable whether the server could not be reached at all, as opposed to
   *     answering with a failure status
   */
  public ValueSetLookupException(
      @Nonnull final String reason,
      @Nonnull final Throwable cause,
      final boolean serverUnreachable) {
    super(reason, cause);
    this.serverUnreachable = serverUnreachable;
  }

  /**
   * Returns whether the terminology server could not be reached, as opposed to answering the first
   * page with a failure status.
   *
   * @return true if the server could not be reached
   */
  public boolean isServerUnreachable() {
    return serverUnreachable;
  }
}
