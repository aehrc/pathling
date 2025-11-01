package au.csiro.pathling.operations.import_;

import au.csiro.pathling.OperationResponse;
import au.csiro.pathling.library.io.sink.FileInformation;
import au.csiro.pathling.library.io.sink.WriteDetails;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.UrlType;

/**
 * @author Felix Naumann
 */
@Getter
public class ImportResponse implements OperationResponse<Parameters> {

  private final String kickOffRequestUrl;
  private final WriteDetails originalInternalWriteDetails;
  private final WriteDetails modifiedExternalWriteDetails;

  public ImportResponse(final String kickOffRequestUrl, final ImportRequest importRequest,
      final WriteDetails writeDetails) {
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
          ParametersParameterComponent parametersParameterComponent = new ParametersParameterComponent().setName(
                  "output")
              .addPart()
              .setName("input").setValue(new UrlType(fileInfo.absoluteUrl()));
          if (fileInfo.count() != null) {
            parametersParameterComponent.addPart().setName("count")
                .setValue(new IntegerType(fileInfo.count()));
          }
          return parametersParameterComponent;
        })
        .forEach(parameters::addParameter);
    return parameters;
  }

  private WriteDetails remapFilesIn(ImportRequest importRequest, WriteDetails writeDetails) {
    // The import request response should not list the file URL for the imported files in Pathling.
    // Rather, it should map to the input file URL provided in the request.
    // Furthermore, Spark may create several files for the same resource type, aggregate them back together.

    // Aggregate counts by resource type.
    Map<String, Long> aggregatedCounts = writeDetails.fileInfos().stream()
        .collect(Collectors.groupingBy(
            FileInformation::fhirResourceType,
            Collectors.summingLong(fi -> fi.count() != null
                                         ? fi.count()
                                         : 0L)
        ));

    // Create FileInformation entries for each input file URL, with aggregated count per resource type.
    List<FileInformation> mappedFileInfos = aggregatedCounts.entrySet().stream()
        .flatMap(entry -> {
          String resourceType = entry.getKey();
          Long totalCount = entry.getValue();
          // Get the collection of input URLs for this resource type.
          return importRequest.input().getOrDefault(resourceType, List.of()).stream()
              .map(inputUrl -> new FileInformation(resourceType, inputUrl, totalCount));
        })
        .toList();

    return new WriteDetails(mappedFileInfos);
  }
}
