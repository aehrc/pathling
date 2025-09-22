package au.csiro.pathling.export;

import au.csiro.pathling.OperationResponse;
import au.csiro.pathling.library.io.sink.DataSink;
import au.csiro.pathling.library.io.sink.NdjsonWriteDetails;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.node.ArrayNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import org.hl7.fhir.r4.model.Binary;
import org.hl7.fhir.r4.model.InstantType;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.function.UnaryOperator;

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

        String baseServerUrl = requestUrl.split("fhir")[0];
        UnaryOperator<String> localUrlToRemoteUrl = localUrl -> baseServerUrl + "jobs" + URI.create(localUrl).getPath().split("jobs")[1];
        
      //final String transactionTime = DateTimeFormatter.ISO_INSTANT.format(LocalDateTime.now());
      //manifest.put("transactionTime", transactionTime); // TODO - is the transactionTime "now"?
        manifest.put("transactionTime", InstantType.now().getValueAsString());
        manifest.put("request", requestUrl);
        manifest.put("requiresAccessToken", false); // TODO - no auth, correct?
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
