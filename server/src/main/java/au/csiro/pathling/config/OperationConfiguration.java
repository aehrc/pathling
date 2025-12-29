/*
 * Copyright 2025 Commonwealth Scientific and Industrial Research
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

import lombok.Data;

/**
 * Configuration for enabling/disabling individual server operations. All operations are enabled by
 * default.
 *
 * @author John Grimes
 */
@Data
public class OperationConfiguration {

  /** Enables CRUD create operations. */
  private boolean createEnabled = true;

  /** Enables CRUD read operations. */
  private boolean readEnabled = true;

  /** Enables CRUD update operations. */
  private boolean updateEnabled = true;

  /** Enables CRUD delete operations. */
  private boolean deleteEnabled = true;

  /** Enables CRUD search operations. */
  private boolean searchEnabled = true;

  /** Enables batch/transaction bundle operations. */
  private boolean batchEnabled = true;

  /** Enables system-level $export operation. */
  private boolean exportEnabled = true;

  /** Enables Patient-level $export operation. */
  private boolean patientExportEnabled = true;

  /** Enables Group-level $export operation. */
  private boolean groupExportEnabled = true;

  /** Enables $import operation. */
  private boolean importEnabled = true;

  /** Enables $import-pnp operation. */
  private boolean importPnpEnabled = true;

  /** Enables system-level $viewdefinition-run operation. */
  private boolean viewDefinitionRunEnabled = true;

  /** Enables instance-level $run operation on ViewDefinition. */
  private boolean viewDefinitionInstanceRunEnabled = true;

  /** Enables $viewdefinition-export operation. */
  private boolean viewDefinitionExportEnabled = true;

  /** Enables $bulk-submit operation. */
  private boolean bulkSubmitEnabled = true;

  /**
   * Returns true if any export operation is enabled.
   *
   * @return true if system, patient, or group export is enabled
   */
  public boolean isAnyExportEnabled() {
    return exportEnabled || patientExportEnabled || groupExportEnabled;
  }
}
