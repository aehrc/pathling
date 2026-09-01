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

package au.csiro.pathling.vcl;

import jakarta.annotation.Nonnull;
import java.io.Serial;
import lombok.Value;

/**
 * A top-level value set inclusion ({@code ^URI}), mapping to a {@code compose.include.valueSet}
 * entry. When {@code codeSystem} is true the reference was written as {@code ^(systemUri)},
 * denoting all concepts of that code system rather than a named value set.
 *
 * @author John Grimes
 */
@Value
public class VclIncludeValueSet implements VclExpression {

  @Serial private static final long serialVersionUID = 1L;

  /** The value set (or code system) URI to include. */
  @Nonnull String uri;

  /** True if the reference is a code system URI ({@code ^(systemUri)}) rather than a value set. */
  boolean codeSystem;
}
