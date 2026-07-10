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

import jakarta.annotation.Nullable;
import jakarta.validation.constraints.Min;
import java.io.Serial;
import java.io.Serializable;
import lombok.Builder;
import lombok.Data;

/**
 * Configuration for local terminology mode, describing where the terminology store lives and how it
 * is used. Required when {@link TerminologyConfiguration#getMode()} is {@link
 * TerminologyMode#LOCAL}.
 *
 * @author John Grimes
 */
@Data
@Builder
public class LocalTerminologyConfiguration implements Serializable {

  @Serial private static final long serialVersionUID = 4470306870886711570L;

  /**
   * The location of the terminology store. Any path accessible through the Hadoop FileSystem API is
   * supported (for example {@code file://}, {@code s3a://}, {@code abfs://}, {@code hdfs://}).
   * Required when the terminology mode is local; the requirement is enforced by the class-level
   * validation on {@link TerminologyConfiguration}.
   */
  @Nullable private String storagePath;

  /**
   * The SNOMED CT module identifier of the edition to prefer when resolving unversioned SNOMED
   * references and more than one edition is present in the store (for example {@code
   * 32506021000036107} for the Australian edition). When absent, an unversioned reference in the
   * presence of multiple editions resolves to an ambiguity error.
   */
  @Nullable private String defaultSnomedEdition;

  /** The maximum number of value set expansions cached per executor JVM. */
  @Min(1)
  @Builder.Default
  private int expansionCacheSize = 100;
}
