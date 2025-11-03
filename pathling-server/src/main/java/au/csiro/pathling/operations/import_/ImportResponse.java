package au.csiro.pathling.operations.import_;

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

  public ImportResponse(final String kickOffRequestUrl, final ImportRequest importRequest,
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
        .map(inputUrl -> {
          final ParametersParameterComponent outputParam = new ParametersParameterComponent()
              .setName("output");
          outputParam.addPart()
              .setName("inputUrl")
              .setValue(new UrlType(inputUrl));
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
    return importRequest.input().values().stream()
        .flatMap(java.util.Collection::stream)
        .toList();
  }
}
