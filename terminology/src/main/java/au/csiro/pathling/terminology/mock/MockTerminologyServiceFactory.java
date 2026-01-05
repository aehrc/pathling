/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.terminology.mock;

import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.utilities.ObjectHolder;
import jakarta.annotation.Nonnull;
import java.io.Serial;

/** This factory is used from the Python tests. */
@SuppressWarnings("unused")
public class MockTerminologyServiceFactory implements TerminologyServiceFactory {

  @Serial private static final long serialVersionUID = 7662506865030951736L;

  private static final ObjectHolder<String, TerminologyService> service =
      ObjectHolder.singleton(c -> new MockTerminologyService());

  @Nonnull
  @Override
  public TerminologyService build() {
    return service.getOrCreate("mock");
  }
}
