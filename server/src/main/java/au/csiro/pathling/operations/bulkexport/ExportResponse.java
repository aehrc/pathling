package au.csiro.pathling.operations.bulkexport;

import au.csiro.pathling.OperationResponse;
import au.csiro.pathling.library.io.sink.WriteDetails;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.node.ArrayNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.function.UnaryOperator;
import org.apache.http.client.utils.URIBuilder;
import org.hl7.fhir.r4.model.Binary;
import org.hl7.fhir.r4.model.InstantType;

/**
 * @author Felix Naumann
 */
public class ExportResponse implements OperationResponse<Binary> {

  private final ObjectMapper mapper = new ObjectMapper();

  private final String kickOffRequestUrl;
  private final String serverBaseUrl;
  private final WriteDetails writeDetails;
  private final boolean requiresAccessToken;

  /**
   * Creates a new ExportResponse.
   *
   * @param kickOffRequestUrl the original export request URL (used in the manifest)
   * @param serverBaseUrl the FHIR server base URL (used for constructing result URLs)
   * @param writeDetails the write details containing file information
   * @param requiresAccessToken whether access token is required to retrieve results
   */
  public ExportResponse(final String kickOffRequestUrl, final String serverBaseUrl,
      final WriteDetails writeDetails, final boolean requiresAccessToken) {
    this.kickOffRequestUrl = kickOffRequestUrl;
    this.serverBaseUrl = serverBaseUrl;
    this.writeDetails = writeDetails;
    this.requiresAccessToken = requiresAccessToken;
  }

  @Override
  public Binary toOutput() {

    String manifestJSON = null;
    try {
      manifestJSON = buildManifest(kickOffRequestUrl, serverBaseUrl, writeDetails);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    Binary binary = new Binary();
    binary.setContentType("application/json");
    binary.setData(manifestJSON.getBytes(StandardCharsets.UTF_8));
    return binary;
  }

  private String buildManifest(String requestUrl, String baseServerUrl, WriteDetails writeDetails)
      throws IOException {
    ObjectNode manifest = mapper.createObjectNode();

    // Ensure the base URL ends with a slash for proper URL construction.
    final String normalizedBaseUrl =
        baseServerUrl.endsWith("/")
        ? baseServerUrl
        : baseServerUrl + "/";
    UnaryOperator<String> localUrlToRemoteUrl = localUrl -> {
      try {
        String[] parts = localUrl.split("/jobs/")[1].split("/");
        String jobUUID = parts[0];
        String file = parts[1];

        return new URIBuilder(normalizedBaseUrl + "$result")
            .addParameter("job", jobUUID)
            .addParameter("file", file)
            .build()
            .toString();
      } catch (URISyntaxException e) {
        throw new InternalErrorException(e);
      }
    };

    manifest.put("transactionTime", InstantType.now().getValueAsString());
    manifest.put("request", requestUrl);
    manifest.put("requiresAccessToken", requiresAccessToken);
    ArrayNode outputArray = mapper.createArrayNode();
    List<ObjectNode> objectNodes = writeDetails.fileInfos().stream()
        .filter(fileInfo -> fileInfo.count() == null || fileInfo.count() > 0)
        .map(fileInfo -> mapper.createObjectNode()
            .put("type", fileInfo.fhirResourceType())
            .put("url", localUrlToRemoteUrl.apply(fileInfo.absoluteUrl()))
            //.put("url", fileInfo.absoluteUrl())
            .put("count", fileInfo.count())
        )
        .toList();
    outputArray.addAll(objectNodes);
    manifest.set("output", outputArray);
    // not supported (for now) but required
    manifest.set("deleted", mapper.createArrayNode());
    manifest.set("error", mapper.createArrayNode());
    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(manifest);
  }

  public String getKickOffRequestUrl() {
    return kickOffRequestUrl;
  }

  public WriteDetails getWriteDetails() {
    return writeDetails;
  }

  public boolean isRequiresAccessToken() {
    return requiresAccessToken;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExportResponse that = (ExportResponse) o;
    return requiresAccessToken == that.requiresAccessToken
        && Objects.equals(kickOffRequestUrl, that.kickOffRequestUrl)
        && Objects.equals(writeDetails, that.writeDetails);
  }

  @Override
  public int hashCode() {
    return Objects.hash(kickOffRequestUrl, writeDetails, requiresAccessToken);
  }

  @Override
  public String toString() {
    return "ExportResponse{"
        + "kickOffRequestUrl='" + kickOffRequestUrl + '\''
        + ", writeDetails=" + writeDetails
        + ", requiresAccessToken=" + requiresAccessToken
        + '}';
  }
}
