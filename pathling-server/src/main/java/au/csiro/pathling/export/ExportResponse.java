package au.csiro.pathling.export;

import au.csiro.pathling.OperationResponse;
import au.csiro.pathling.library.io.sink.DataSink;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.node.ArrayNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import org.hl7.fhir.r4.model.Binary;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * @author Felix Naumann
 */
public class ExportResponse implements OperationResponse<Binary> {

    private final ObjectMapper mapper = new ObjectMapper();

    private final String kickOffRequestUrl;
    private final DataSink.NdjsonWriteDetails writeDetails;

    public ExportResponse(String kickOffRequestUrl, DataSink.NdjsonWriteDetails writeDetails) {
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

    private String buildManifest(String requestUrl, DataSink.NdjsonWriteDetails writeDetails) throws IOException {
        ObjectNode manifest = mapper.createObjectNode();
        manifest.put("transactionTime", Instant.now().toString()); // TODO - is the transactionTime "now"?
        manifest.put("request", requestUrl);
        manifest.put("requiresAccessToken", false); // TODO - no auth, correct?
        ArrayNode outputArray = mapper.createArrayNode();
        List<ObjectNode> objectNodes = writeDetails.fileInfos().stream()
                .filter(fileInfo -> fileInfo.count() > 0)
                .map(fileInfo -> mapper.createObjectNode()
                        .put("type", fileInfo.fhirResourceType())
                        .put("url", fileInfo.absoluteUrl())
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

    public DataSink.NdjsonWriteDetails getWriteDetails() {
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
