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

package au.csiro.pathling.util;

import au.csiro.pathling.library.io.sink.WriteDetails;
import au.csiro.pathling.operations.bulkexport.ExportExecutor;
import au.csiro.pathling.operations.bulkexport.ExportResponse;
import com.google.common.annotations.VisibleForTesting;
import java.util.UUID;
import org.hl7.fhir.r4.model.Parameters;

/**
 * This record is necessary to perform unit tests in the {@link ExportExecutor}. Otherwise, the unit
 * test invoking the execute method does not know the job UUID this belongs to.
 *
 * @param fakeJobId The fake job UUID made available to the testing method.
 * @param exportResponse The actual response from the execute method invocation.
 * @author Felix Naumann
 */
@VisibleForTesting
public record TestExportResponse(UUID fakeJobId, ExportResponse exportResponse) {

  public String getKickOffRequestUrl() {
    return exportResponse.getKickOffRequestUrl();
  }

  public WriteDetails getWriteDetails() {
    return exportResponse.getWriteDetails();
  }

  public Parameters toOutput() {
    return exportResponse.toOutput();
  }
}
