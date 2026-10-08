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
package au.csiro.pathling.library.io.source;

import au.csiro.pathling.errors.InvalidUserInputError;
import jakarta.annotation.Nonnull;
import java.io.Serial;
import lombok.Getter;

/**
 * Thrown when a resource table is stored in a layout this release does not read, such as the layout
 * written by earlier releases. The message names the resource type, the detected layout, the
 * expected layout, the evidence and the remedy.
 *
 * @author Piotr Szul
 */
@Getter
public class UnsupportedLayoutException extends InvalidUserInputError {

  @Serial private static final long serialVersionUID = 2794580374046273817L;

  /** The resource type whose data is in the unsupported layout. */
  @Nonnull private final String resourceType;

  /** The classification that caused the refusal. */
  @Nonnull private final transient LayoutDetection detection;

  /**
   * Creates the exception.
   *
   * @param message the actionable message
   * @param resourceType the resource type whose data is in the unsupported layout
   * @param detection the classification that caused the refusal
   */
  public UnsupportedLayoutException(
      @Nonnull final String message,
      @Nonnull final String resourceType,
      @Nonnull final LayoutDetection detection) {
    super(message);
    this.resourceType = resourceType;
    this.detection = detection;
  }
}
