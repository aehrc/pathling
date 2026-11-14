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

package au.csiro.pathling.schema;

import au.csiro.pathling.definition.ChildDefinition;
import au.csiro.pathling.definition.NodeDefinition;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

/**
 * A node definition that counts the traversals made through it, so that a test can assert that a
 * structure expands only what it is asked for.
 */
class CountingNodeDefinition implements NodeDefinition {

  @Nonnull private final NodeDefinition delegate;

  private int childrenCalls;

  CountingNodeDefinition(@Nonnull final NodeDefinition delegate) {
    this.delegate = delegate;
  }

  /**
   * Returns the number of times the children of this node have been enumerated.
   *
   * @return the number of enumerations
   */
  int getChildrenCalls() {
    return childrenCalls;
  }

  @Nonnull
  @Override
  public Optional<ChildDefinition> getChildElement(@Nonnull final String name) {
    return delegate.getChildElement(name);
  }

  @Nonnull
  @Override
  public List<ChildDefinition> getChildren() {
    childrenCalls++;
    return delegate.getChildren();
  }

  @Override
  public boolean isFhirDefinition() {
    return delegate.isFhirDefinition();
  }

  @Nonnull
  @Override
  public Object getTypeIdentity() {
    return delegate.getTypeIdentity();
  }
}
