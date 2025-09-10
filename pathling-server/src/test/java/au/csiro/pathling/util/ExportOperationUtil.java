package au.csiro.pathling.util;

import au.csiro.pathling.export.ExportOutputFormat;
import au.csiro.pathling.export.ExportRequest;
import au.csiro.pathling.export.ExportResponse;
import au.csiro.pathling.library.io.sink.DataSink;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.JsonNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.node.ArrayNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import ca.uhn.fhir.parser.IParser;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations;
import org.hl7.fhir.r4.model.InstantType;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Felix Naumann
 */
public class ExportOperationUtil {

    public static JsonNode json(ObjectMapper mapper, String request, DataSink.NdjsonWriteDetails writeDetails) {
        return json(mapper, InstantType.now(), request, false, writeDetails.fileInfos());
    }

    public static JsonNode json(ObjectMapper mapper, InstantType transactionTime, String request, boolean requiresAccessToken, List<DataSink.FileInfo> output) {
        ObjectNode node = mapper.createObjectNode();
        node.put("transactionTime", transactionTime.toString());
        node.put("request", request);
        node.put("requiresAccessToken", requiresAccessToken);
        List<ObjectNode> outputList = output.stream()
                .map(fileInfo -> mapper.createObjectNode()
                        .put("type", fileInfo.fhirResourceType())
                        .put("url", fileInfo.absoluteUrl())
                        .put("count", fileInfo.count())
                )
                .toList();
        ArrayNode arrayNode = mapper.createArrayNode().addAll(outputList);
        node.set("output", arrayNode);
        node.set("deleted", mapper.createArrayNode());
        node.set("error", mapper.createArrayNode());
        return node;
    }

    public static ExportResponse res(ExportRequest request, DataSink.NdjsonWriteDetails writeDetails) {
        return new ExportResponse(request.originalRequest(), writeDetails);
    }

    public static ExportRequest req(String base,
                                    ExportOutputFormat outputFormat,
                                    InstantType since,
                                    List<Enumerations.ResourceType> includeResourceTypeFilters) {
        String originalRequest = base + "_outputFormat=" + ExportOutputFormat.asParam(outputFormat) + "&_since=" + since.toString();
        if(!includeResourceTypeFilters.isEmpty()) {
            originalRequest += "&_type=" + includeResourceTypeFilters.stream().map(Enumerations.ResourceType::toCode).collect(Collectors.joining(","));
        }
        return new ExportRequest(originalRequest, outputFormat, since, null, includeResourceTypeFilters, List.of());
    }

    public static ExportRequest req(String base, List<String> elements) {
        return req(base, List.of(), elements);
    }

    public static ExportRequest req(String base, List<Enumerations.ResourceType> includeResourceTypeFilters, List<String> elements) {
        InstantType now = InstantType.now();
        String originalRequest = base + "_outputFormat=" + ExportOutputFormat.asParam(ExportOutputFormat.ND_JSON) + "&_since=" + now;

        if(!includeResourceTypeFilters.isEmpty()) {
            originalRequest += "&_type=" + includeResourceTypeFilters.stream().map(Enumerations.ResourceType::toCode).collect(Collectors.joining(","));
        }

        originalRequest += "&_elements=" + String.join(",", elements);
        List<ExportRequest.FhirElement> fhirElements = new ArrayList<>();
        for(String el : elements) {
            String[] split = el.split("\\.");
            if(split.length == 1) {
                fhirElements.add(new ExportRequest.FhirElement(null, split[0]));
            }
            if(split.length == 2) {
                fhirElements.add(new ExportRequest.FhirElement(Enumerations.ResourceType.fromCode(split[0]), split[1]));
            }
        }
        return new ExportRequest(originalRequest, ExportOutputFormat.ND_JSON, now, null, List.of(), fhirElements);
    }

    public static DataSink.NdjsonWriteDetails write_details(List<DataSink.FileInfo> fileInfos) {
        return new DataSink.NdjsonWriteDetails(fileInfos);
    }

    public static DataSink.NdjsonWriteDetails write_details(DataSink.FileInfo... fileInfos) {
        return new DataSink.NdjsonWriteDetails(Arrays.asList(fileInfos));
    }

    public static DataSink.FileInfo fi(String fhirResourceType, String absoluteUrl, long count) {
        return new DataSink.FileInfo(fhirResourceType, absoluteUrl, count);
    }

    public static InstantType date(String date) {
        return new InstantType(date + "T00:00:00Z");
    }

    public static <T extends IBaseResource> T read_first_from_multiple_lines_ndjson(IParser parser, DataSink.FileInfo fileInfo, Class<T> clazz) {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(URI.create(fileInfo.absoluteUrl())))) {
            String firstLine = reader.readLine();
            if (firstLine != null && !firstLine.trim().isEmpty()) {
                // Parse firstLine as JSON
                return parser.parseResource(clazz, firstLine);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        throw new RuntimeException();
    }

    public static <T extends IBaseResource> T read_ndjson(IParser parser, DataSink.FileInfo fileInfo, Class<T> clazz) throws IOException {
        return parser.parseResource(clazz, Files.readString(Paths.get(URI.create(fileInfo.absoluteUrl()))));
    }
}
