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

package au.csiro.pathling.operations.bulkimport;

import au.csiro.pathling.OperationResponse;
import au.csiro.pathling.library.io.sink.WriteDetails;
import java.util.List;
import lombok.Getter;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.UrlType;

/**
 * Represents the response from an import operation, aligned with the SMART Bulk Data Import
 * specification.
 *
 * @author Felix Naumann
 */
@Getter
public class ImportResponse implements OperationResponse<Parameters> {

  private final String kickOffRequestUrl;
  private final WriteDetails originalInternalWriteDetails;
  private final List<String> inputUrls;

  /**
   * Creates a new ImportResponse.
   *
   * @param kickOffRequestUrl the original kick-off request URL
   * @param importRequest the import request that was processed
   * @param writeDetails the details of the write operation
   */
  public ImportResponse(
      final String kickOffRequestUrl,
      final ImportRequest importRequest,
      final WriteDetails writeDetails) {
    this.kickOffRequestUrl = kickOffRequestUrl;
    this.originalInternalWriteDetails = writeDetails;
    this.inputUrls = extractInputUrls(importRequest);
  }

  @Override
  public Parameters toOutput() {
    final Parameters parameters = new Parameters();
    parameters.addParameter("transactionTime", InstantType.now().getValueAsString());
    parameters.addParameter("request", kickOffRequestUrl);

    // Add output entries with inputUrl field for each input file.
    inputUrls.stream()
        .map(
            inputUrl -> {
              final ParametersParameterComponent outputParam =
                  new ParametersParameterComponent().setName("output");
              outputParam.addPart().setName("inputUrl").setValue(new UrlType(inputUrl));
              return outputParam;
            })
        .forEach(parameters::addParameter);

    return parameters;
  }

  /**
   * Extracts all input URLs from the import request.
   *
   * @param importRequest the import request
   * @return a flat list of all input URLs
   */
  private List<String> extractInputUrls(final ImportRequest importRequest) {
    return importRequest.input().values().stream().flatMap(java.util.Collection::stream).toList();
  }
}
