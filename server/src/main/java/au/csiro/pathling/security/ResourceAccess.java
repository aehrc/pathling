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

package au.csiro.pathling.security;

import jakarta.annotation.Nonnull;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import lombok.Getter;

/**
 * Identifies methods that implement access to underlying Resources.
 *
 * @author Piotr Szul
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface ResourceAccess {

  /**
   * The type of access implemented, e.g. read or write.
   *
   * @return The type of access.
   */
  AccessType value();

  /** Types of access. */
  @Getter
  enum AccessType {
    /** Read access. */
    READ("read"),
    /** Write access (does not subsume read access). */
    WRITE("write");

    @Nonnull private final String code;

    AccessType(@Nonnull final String code) {
      this.code = code;
    }
  }
}
