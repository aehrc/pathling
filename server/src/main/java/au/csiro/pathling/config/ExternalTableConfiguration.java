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

package au.csiro.pathling.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.Data;

/**
 * One operator-configured external table that a SQLQuery or SQLView may declare as a dependency and
 * read by its label. Bound from {@code pathling.sqlQuery.externalTables.N.*}.
 *
 * <p>Validation here is purely structural and happens at bind time; no path is probed at startup,
 * so an unreachable store or late-bound credentials never prevent the server from starting.
 *
 * @author John Grimes
 */
@Data
public class ExternalTableConfiguration {

  /**
   * The canonical URL that a Library's {@code relatedArtifact.resource} uses to reference this
   * table. Must be unique across the configured tables and must not contain {@code |}, which is the
   * separator between a canonical URL and its version pin and so could never be referenced.
   */
  @NotBlank
  @Pattern(regexp = "[^|]+")
  private String url;

  /**
   * The storage location of the table, accepting the same filesystem schemes as the warehouse URL
   * ({@code file://}, {@code s3a://}, {@code hdfs://}).
   */
  @NotBlank private String path;

  /**
   * The Spark data source name passed to {@code DataFrameReader.format}: {@code delta} or {@code
   * parquet}.
   */
  @NotBlank
  @Pattern(regexp = "delta|parquet")
  private String format = "delta";
}
