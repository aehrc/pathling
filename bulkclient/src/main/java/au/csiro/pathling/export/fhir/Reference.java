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

package au.csiro.pathling.export.fhir;

import java.net.URI;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class Reference {

  @Nullable
  String reference;

  @Nullable
  String type;

  @Nullable
  String identifier;

  @Nullable
  String display;

  @SuppressWarnings("unused")
  public static class ReferenceBuilder {

    ReferenceBuilder identifierFromUri(@Nonnull final URI uri) {
      return identifier(uri.toString());
    }
  }

  @Nonnull
  public static Reference of(@Nonnull final String reference) {
    return Reference.builder().reference(reference).build();
  }
}