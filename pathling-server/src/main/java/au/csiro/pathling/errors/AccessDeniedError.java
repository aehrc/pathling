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

package au.csiro.pathling.errors;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.Getter;

/**
 * Thrown when invalid user input is detected, and we want to send the details of the problem back
 * to the user.
 *
 * @author John Grimes
 */
@Getter
public class AccessDeniedError extends RuntimeException {

  private static final long serialVersionUID = -4574080049289748708L;

  @Nullable
  private final String missingAuthority;

  /**
   * @param message the detail message for the error
   */
  public AccessDeniedError(@Nonnull final String message) {
    super(message);
    this.missingAuthority = null;
  }

  /**
   * @param message the detail message for the error
   * @param missingAuthority the missing authority that caused the error
   */
  public AccessDeniedError(@Nonnull final String message, @Nonnull final String missingAuthority) {
    super(message);
    this.missingAuthority = missingAuthority;
  }

}
