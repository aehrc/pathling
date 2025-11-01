package au.csiro.pathling.operations.import_;

import au.csiro.pathling.OperationResponse;
import au.csiro.pathling.library.io.sink.FileInfo;
import au.csiro.pathling.library.io.sink.WriteDetails;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.UrlType;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Felix Naumann
 */
@Getter
public class ImportResponse implements OperationResponse<Parameters> {

  private final String kickOffRequestUrl;
  private final WriteDetails originalInternalWriteDetails;
  private final WriteDetails modifiedExternalWriteDetails;

  public ImportResponse(final String kickOffRequestUrl, final ImportRequest importRequest, final WriteDetails writeDetails) {
    this.kickOffRequestUrl = kickOffRequestUrl;
    this.originalInternalWriteDetails = writeDetails;
    this.modifiedExternalWriteDetails = remapFilesIn(importRequest, writeDetails);
  }

  @Override
  public Parameters toOutput() {
    Parameters parameters = new Parameters();
    parameters.addParameter("transactionTime", InstantType.now().getValueAsString());
    parameters.addParameter("request", kickOffRequestUrl);
    modifiedExternalWriteDetails.fileInfos().stream()
        .map(fileInfo -> {
          ParametersParameterComponent parametersParameterComponent = new ParametersParameterComponent().setName("output")
              .addPart()
              .setName("input").setValue(new UrlType(fileInfo.absoluteUrl()));
          if(fileInfo.count() != null) {
            parametersParameterComponent.addPart().setName("count").setValue(new IntegerType(fileInfo.count()));
          }
          return parametersParameterComponent;
        })
        .forEach(parameters::addParameter);
    return parameters;
  }
  
  private WriteDetails remapFilesIn(ImportRequest importRequest, WriteDetails writeDetails) {
    // the import request response should not list the file url for the imported files in pathling.
    // Rather, it should map to the input file url provided in the request.
    // Furthermore, spark may create several files for the same resource type, aggregate them back together.
    
    // Spark may create multiple files for the same resource type, so we need to aggregate them
    Map<String, Long> aggregatedCounts = writeDetails.fileInfos().stream()
        .collect(Collectors.groupingBy(
            FileInfo::fhirResourceType,
            Collectors.summingLong(fi -> fi.count() != null ? fi.count() : 0L)
        ));

    List<FileInfo> mappedFileInfos = aggregatedCounts.entrySet().stream()
        .map(entry -> new FileInfo(
            entry.getKey(),
            importRequest.input().get(entry.getKey()),
            entry.getValue()
        ))
        .toList();

    return new WriteDetails(mappedFileInfos);
  }
}
