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

package au.csiro.pathling.test.helpers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.r4.model.Base;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

public class FhirDeepMatcher<T extends Base> implements ArgumentMatcher<T> {

  @Nonnull
  private final T expected;

  private FhirDeepMatcher(@Nonnull final T expected) {
    this.expected = expected;
  }

  @Override
  public boolean matches(@Nullable final T actual) {
    return expected.equalsDeep(actual);
  }

  public static <T extends Base> T deepEq(@Nonnull final T expected) {
    return Mockito.argThat(new FhirDeepMatcher<>(expected));
  }

}
