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

package au.csiro.pathling.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import lombok.Data;

/**
 * Represents configuration relating to Cross-Origin Resource Sharing (CORS).
 */
@Data
public class CorsConfiguration {

  @NotNull
  private List<String> allowedOrigins;

  @NotNull
  private List<String> allowedOriginPatterns;

  @NotNull
  private List<String> allowedMethods;

  @NotNull
  private List<String> allowedHeaders;

  @NotNull
  private List<String> exposedHeaders;

  @NotNull
  @Min(0)
  private Long maxAge;

}
