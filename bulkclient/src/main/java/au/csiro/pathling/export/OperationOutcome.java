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

package au.csiro.pathling.export;

import static java.util.Objects.nonNull;

import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;

@Value
public class OperationOutcome {

  @Value
  static class Issue {

    @Nullable
    String code;
  }

  @Nullable
  String resourceType;

  @Nullable
  List<Issue> issue;

  @Nonnull
  public static Optional<OperationOutcome> parse(@Nonnull final String jsonString) {
    return GsonSupport.fromJson(jsonString, OperationOutcome.class)
        .filter(ou -> "OperationOutcome".equals(ou.getResourceType()));
  }

  public boolean isTransient() {
    return nonNull(getIssue())
        && getIssue().stream().map(Issue::getCode)
        .anyMatch("transient"::equals);
  }

}