package au.csiro.pathling.operations.export;

import au.csiro.pathling.OperationResponse;
import au.csiro.pathling.library.io.sink.NdjsonWriteDetails;
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
    private final NdjsonWriteDetails writeDetails;

    public ExportResponse(String kickOffRequestUrl, NdjsonWriteDetails writeDetails) {
        this.kickOffRequestUrl = kickOffRequestUrl;
        this.writeDetails = writeDetails;
    }

    @Override
    public Binary toOutput() {

        String manifestJSON = null;
        try {
            manifestJSON = buildManifest(kickOffRequestUrl, writeDetails);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        Binary binary = new Binary();
        binary.setContentType("application/json");
        binary.setData(manifestJSON.getBytes(StandardCharsets.UTF_8));
        return binary;
    }

    private String buildManifest(String requestUrl, NdjsonWriteDetails writeDetails) throws IOException {
        ObjectNode manifest = mapper.createObjectNode();

        String baseServerUrl = requestUrl.split("\\$export")[0];
        UnaryOperator<String> localUrlToRemoteUrl = localUrl -> {
          try {
            String[] parts = localUrl.split("/jobs/")[1].split("/");
            String jobUUID = parts[0];
            String file = parts[1];
            
            return new URIBuilder(baseServerUrl + "$result")
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
        manifest.put("requiresAccessToken", false);
        ArrayNode outputArray = mapper.createArrayNode();
        List<ObjectNode> objectNodes = writeDetails.fileInfos().stream()
                .filter(fileInfo -> fileInfo.count() == null || fileInfo.count() > 0)
                .map(fileInfo -> mapper.createObjectNode()
                        .put("type", fileInfo.fhirResourceType())
                        .put("url",  localUrlToRemoteUrl.apply(fileInfo.absoluteUrl()))
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

    public NdjsonWriteDetails getWriteDetails() {
        return writeDetails;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ExportResponse that = (ExportResponse) o;
        return Objects.equals(kickOffRequestUrl, that.kickOffRequestUrl) && Objects.equals(writeDetails, that.writeDetails);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kickOffRequestUrl, writeDetails);
    }

    @Override
    public String toString() {
        return "ExportResponse{" +
                "kickOffRequestUrl='" + kickOffRequestUrl + '\'' +
                ", writeDetails=" + writeDetails +
                '}';
    }
}
