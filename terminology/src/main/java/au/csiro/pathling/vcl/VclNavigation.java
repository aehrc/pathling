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
 * A reverse property navigation of the form {@code source.property} (read as "the property values
 * of the source concepts"). The source is a code, code list, wildcard, URI or nested filter list.
 *
 * @author John Grimes
 */
@Value
public class VclNavigation implements VclExpression {

  @Serial private static final long serialVersionUID = 1L;

  /** The concepts whose property is read. */
  @Nonnull VclFilterValue source;

  /** The property navigated to. */
  @Nonnull String property;
}
