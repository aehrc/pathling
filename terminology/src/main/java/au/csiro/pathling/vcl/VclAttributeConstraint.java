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
 * Selects the concepts that have an attribute relationship of a given type whose value falls in a
 * value set. It is produced by translating an ECL attribute constraint ({@code attr = value}).
 *
 * <p>Unlike a property filter, the attribute type is a relationship type code that is not
 * necessarily a stored concept, so it is held as a raw code rather than routed through the concept
 * dictionary. When {@code includeAttributeDescendants} is set (an ECL hierarchy operator on the
 * attribute name, e.g. {@code << 363698007}), the constraint also matches every descendant
 * attribute type; {@code includeAttributeSelf} controls whether the named type itself is matched,
 * so descendants-or-self, descendants-only, and exact selection are all expressible.
 *
 * @author John Grimes
 */
@Value
public class VclAttributeConstraint implements VclExpression {

  @Serial private static final long serialVersionUID = 1L;

  /** The attribute type code (a relationship type). */
  @Nonnull String attributeType;

  /** Whether the named attribute type itself is matched. */
  boolean includeAttributeSelf;

  /** Whether the descendants of the named attribute type are also matched. */
  boolean includeAttributeDescendants;

  /** Whether the constraint is negated (an ECL {@code !=} comparison). */
  boolean negated;

  /** The value set the attribute must point into. */
  @Nonnull VclExpression value;
}
