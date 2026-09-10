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
import jakarta.annotation.Nullable;
import java.io.Serial;
import java.io.Serializable;
import lombok.Value;

/**
 * A code system URI, with an optional version pinned after a {@code |}. Selects the code system
 * that a scoped expression is evaluated against.
 *
 * @author John Grimes
 */
@Value
public class VclSystemUri implements Serializable {

  @Serial private static final long serialVersionUID = 1L;

  /** The code system URI. */
  @Nonnull String system;

  /** The pinned version, or null if unversioned. */
  @Nullable String version;
}
