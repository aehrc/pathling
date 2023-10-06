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

package au.csiro.pathling.view;

import javax.annotation.Nonnull;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import lombok.EqualsAndHashCode;
import lombok.Value;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Value
public class FromSelection extends AbstractCompositeSelection {

  public FromSelection(final FhirPath<Collection> parent, final List<Selection> components) {
    super(parent, components);
  }

  @Nonnull
  @Override
  protected String getName() {
    return "from";
  }

  @Nonnull
  @Override
  protected FromSelection copy(@Nonnull final List<Selection> newComponents) {
    return new FromSelection(path, newComponents);
  }
}
