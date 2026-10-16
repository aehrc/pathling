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

import java.io.Serial;

/**
 * Thrown when the membership of a value set exceeds the largest membership the caller will accept.
 * A partial membership is never returned, so the caller learns of the excess only through this
 * exception.
 *
 * @author John Grimes
 */
public class ExpansionLimitExceededException extends ValueSetExpansionException {

  @Serial private static final long serialVersionUID = 6019483306273156210L;

  private final int limit;

  /**
   * Creates an exception for a membership that exceeds the given limit.
   *
   * @param limit the largest membership the caller would accept
   */
  public ExpansionLimitExceededException(final int limit) {
    super("the value set has more than the maximum of " + limit + " members");
    this.limit = limit;
  }

  /**
   * Returns the limit that was exceeded.
   *
   * @return the largest membership the caller would accept
   */
  public int getLimit() {
    return limit;
  }
}
