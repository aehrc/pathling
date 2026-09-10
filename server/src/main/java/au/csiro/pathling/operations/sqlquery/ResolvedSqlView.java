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

package au.csiro.pathling.operations.sqlquery;

import jakarta.annotation.Nonnull;
import java.util.Map;
import lombok.Value;

/**
 * A resolved {@code SQLView} node. Carries the view's SQL and the mapping from each table label the
 * SQL uses to the canonical key of the child node that label resolves to, so the SQL can be
 * rewritten against the children's request-scoped temp views at materialisation time.
 *
 * @author John Grimes
 */
@Value
public class ResolvedSqlView implements ResolvedDependency {

  /** The stable canonical identity of the SQLView Library. */
  @Nonnull String canonicalKey;

  /** The view's SQL text. */
  @Nonnull String sql;

  /** This view's local table label to the canonical key of the resolved child it references. */
  @Nonnull Map<String, String> childKeysByLabel;
}
