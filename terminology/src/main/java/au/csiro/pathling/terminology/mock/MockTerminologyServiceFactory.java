/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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

import au.csiro.pathling.terminology.TerminologyService2;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.utilities.ObjectHolder;
import javax.annotation.Nonnull;

public class MockTerminologyServiceFactory implements TerminologyServiceFactory {

  private final static ObjectHolder<String, TerminologyService2> service = ObjectHolder.singleton(
      c -> new MockTerminologyService2());
  private static final long serialVersionUID = 7662506865030951736L;
  
  @Nonnull
  @Override
  public TerminologyService2 buildService2() {
    return service.getOrCreate("mock");
  }
}
