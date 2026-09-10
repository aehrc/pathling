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
 * An expression scoped to a particular code system by a {@code (systemUri)} prefix. The system
 * applies to the immediately following simple expression or bracketed group.
 *
 * @author John Grimes
 */
@Value
public class VclSystemScoped implements VclExpression {

  @Serial private static final long serialVersionUID = 1L;

  /** The code system the expression is scoped to. */
  @Nonnull VclSystemUri system;

  /** The scoped expression. */
  @Nonnull VclExpression expression;
}
