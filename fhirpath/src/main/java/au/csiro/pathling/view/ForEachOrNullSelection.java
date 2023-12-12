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

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.view.DatasetResult.One;
import lombok.EqualsAndHashCode;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Value
public class ForEachOrNullSelection extends AbstractCompositeSelection {

  public ForEachOrNullSelection(final FhirPath parent, final List<Selection> components) {
    super(parent, components);
  }

  @Nonnull
  @Override
  protected String getName() {
    return "forEachOrNull";
  }

  @Nonnull
  @Override
  protected ForEachOrNullSelection copy(@Nonnull final List<Selection> newComponents) {
    return new ForEachOrNullSelection(path, newComponents);
  }

  @Nonnull
  @Override
  protected One<ProjectionContext> subContext(@Nonnull final ProjectionContext context,
                                                              @Nonnull final FhirPath parent) {
    return context.subContext(parent, true, true);
  }
}
