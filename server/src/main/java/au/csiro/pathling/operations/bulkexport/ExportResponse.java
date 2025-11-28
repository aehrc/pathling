package au.csiro.pathling.operations.bulkexport;

import au.csiro.pathling.OperationResponse;
import au.csiro.pathling.library.io.sink.WriteDetails;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.node.ArrayNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import org.apache.http.client.utils.URIBuilder;
import org.hl7.fhir.r4.model.Binary;
import org.hl7.fhir.r4.model.InstantType;

/**
 * Represents the response from a bulk export operation, containing the export manifest.
 *
 * @author Felix Naumann
 */
public class ExportResponse implements OperationResponse<Binary> {

  @Nonnull
  private final ObjectMapper mapper = new ObjectMapper();

  @Nonnull
  private final String kickOffRequestUrl;

  @Nonnull
  private final String serverBaseUrl;

  @Nonnull
  private final WriteDetails writeDetails;

  /**
   * Whether an access token is required to retrieve results.
   */
  @Getter
  private final boolean requiresAccessToken;

  /**
   * Creates a new ExportResponse.
   *
   * @param kickOffRequestUrl the original export request URL (used in the manifest)
   * @param serverBaseUrl the FHIR server base URL (used for constructing result URLs)
   * @param writeDetails the write details containing file information
   * @param requiresAccessToken whether access token is required to retrieve results
   */
  public ExportResponse(@Nonnull final String kickOffRequestUrl,
      @Nonnull final String serverBaseUrl, @Nonnull final WriteDetails writeDetails,
      final boolean requiresAccessToken) {
    this.kickOffRequestUrl = kickOffRequestUrl;
    this.serverBaseUrl = serverBaseUrl;
    this.writeDetails = writeDetails;
    this.requiresAccessToken = requiresAccessToken;
  }

  @Nonnull
  @Override
  public Binary toOutput() {
    final String manifestJSON;
    try {
      manifestJSON = buildManifest(kickOffRequestUrl, serverBaseUrl, writeDetails);
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }
    final Binary binary = new Binary();
    binary.setContentType("application/json");
    binary.setData(manifestJSON.getBytes(StandardCharsets.UTF_8));
    return binary;
  }

  @Nonnull
  private String buildManifest(@Nonnull final String requestUrl,
      @Nonnull final String baseServerUrl, @Nonnull final WriteDetails writeDetails)
      throws IOException {
    final ObjectNode manifest = mapper.createObjectNode();

    // Ensure the base URL ends with a slash for proper URL construction.
    final String normalizedBaseUrl =
        baseServerUrl.endsWith("/")
            ? baseServerUrl
            : baseServerUrl + "/";

    manifest.put("transactionTime", InstantType.now().getValueAsString());
    manifest.put("request", requestUrl);
    manifest.put("requiresAccessToken", requiresAccessToken);
    final ArrayNode outputArray = mapper.createArrayNode();
    final List<ObjectNode> objectNodes = writeDetails.fileInfos().stream()
        .filter(fileInfo -> fileInfo.count() == null || fileInfo.count() > 0)
        .map(fileInfo -> mapper.createObjectNode()
            .put("type", fileInfo.fhirResourceType())
            .put("url", buildResultUrl(normalizedBaseUrl, fileInfo.absoluteUrl()))
            .put("count", fileInfo.count())
        )
        .toList();
    outputArray.addAll(objectNodes);
    manifest.set("output", outputArray);
    // Not supported yet but required by the specification.
    manifest.set("deleted", mapper.createArrayNode());
    manifest.set("error", mapper.createArrayNode());
    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(manifest);
  }

  /**
   * Converts a local file URL to a remote result URL.
   *
   * @param baseUrl the normalised base server URL
   * @param localUrl the local file URL containing the job ID and filename
   * @return the remote result URL
   */
  @Nonnull
  private static String buildResultUrl(@Nonnull final String baseUrl,
      @Nonnull final String localUrl) {
    try {
      final String[] parts = localUrl.split("/jobs/")[1].split("/");
      final String jobUUID = parts[0];
      final String file = parts[1];

      return new URIBuilder(baseUrl + "$result")
          .addParameter("job", jobUUID)
          .addParameter("file", file)
          .build()
          .toString();
    } catch (final URISyntaxException e) {
      throw new InternalErrorException(e);
    }
  }

  /**
   * Returns the original kick-off request URL.
   *
   * @return the kick-off request URL
   */
  @Nonnull
  public String getKickOffRequestUrl() {
    return kickOffRequestUrl;
  }

  /**
   * Returns the write details containing file information.
   *
   * @return the write details
   */
  @Nonnull
  public WriteDetails getWriteDetails() {
    return writeDetails;
  }

  @Override
  public boolean equals(final Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ExportResponse that = (ExportResponse) o;
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
