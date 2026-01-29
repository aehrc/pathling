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

package au.csiro.pathling.operations.view;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.compartment.GroupMemberService;
import au.csiro.pathling.operations.compartment.PatientCompartmentService;
import au.csiro.pathling.security.PathlingAuthority;
import au.csiro.pathling.security.ResourceAccess.AccessType;
import au.csiro.pathling.security.SecurityAspect;
import au.csiro.pathling.views.Column;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewExecutor;
import au.csiro.pathling.views.ViewDefinitionGson;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.ConstraintViolationException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.IntegerType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Helper class for executing ViewDefinition queries. Contains shared logic used by both {@link
 * ViewDefinitionRunProvider} and {@link ViewDefinitionInstanceRunProvider}.
 *
 * @author John Grimes
 */
@Slf4j
@Component
public class ViewExecutionHelper {

  @Nonnull private final SparkSession sparkSession;

  @Nonnull private final QueryableDataSource deltaLake;

  @Nonnull private final FhirContext fhirContext;

  @Nonnull private final FhirEncoders fhirEncoders;

  @Nonnull private final PatientCompartmentService patientCompartmentService;

  @Nonnull private final GroupMemberService groupMemberService;

  @Nonnull private final ServerConfiguration serverConfiguration;

  @Nonnull private final Gson gson;

  /**
   * Constructs a new ViewExecutionHelper.
   *
   * @param sparkSession the Spark session
   * @param deltaLake the queryable data source
   * @param fhirContext the FHIR context
   * @param fhirEncoders the FHIR encoders
   * @param patientCompartmentService the patient compartment service
   * @param groupMemberService the group member service
   * @param serverConfiguration the server configuration
   */
  @Autowired
  public ViewExecutionHelper(
      @Nonnull final SparkSession sparkSession,
      @Nonnull final QueryableDataSource deltaLake,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final PatientCompartmentService patientCompartmentService,
      @Nonnull final GroupMemberService groupMemberService,
      @Nonnull final ServerConfiguration serverConfiguration) {
    this.sparkSession = sparkSession;
    this.deltaLake = deltaLake;
    this.fhirContext = fhirContext;
    this.fhirEncoders = fhirEncoders;
    this.patientCompartmentService = patientCompartmentService;
    this.groupMemberService = groupMemberService;
    this.serverConfiguration = serverConfiguration;
    this.gson = ViewDefinitionGson.create();
  }

  /**
   * Executes a ViewDefinition and streams the results to the HTTP response.
   *
   * @param viewResource the ViewDefinition resource to execute
   * @param format the output format (ndjson or csv), overrides Accept header if provided
   * @param acceptHeader the HTTP Accept header value, used as fallback if format is not provided
   * @param includeHeader whether to include a header row in CSV output
   * @param limit the maximum number of rows to return
   * @param patientIds patient IDs to filter by
   * @param groupIds group IDs to filter by
   * @param since filter by meta.lastUpdated >= value
   * @param inlineResources FHIR resources to use instead of the main data source
   * @param response the HTTP response for streaming output
   */
  @SuppressWarnings("java:S107")
  public void executeView(
      @Nonnull final IBaseResource viewResource,
      @Nullable final String format,
      @Nullable final String acceptHeader,
      @Nullable final BooleanType includeHeader,
      @Nullable final IntegerType limit,
      @Nullable final List<String> patientIds,
      @Nullable final List<IdType> groupIds,
      @Nullable final InstantType since,
      @Nullable final List<String> inlineResources,
      @Nullable final HttpServletResponse response) {

    if (response == null) {
      throw new InvalidRequestException("HTTP response is required for this operation");
    }

    // Parse the ViewDefinition.
    final FhirView view = parseViewDefinition(viewResource);

    // Check resource-level authorization if authentication is enabled.
    if (serverConfiguration.getAuth().isEnabled()) {
      SecurityAspect.checkHasAuthority(
          PathlingAuthority.resourceAccess(AccessType.READ, view.getResource()));
    }

    // Determine output format: _format parameter overrides Accept header.
    final ViewOutputFormat outputFormat =
        (format != null && !format.isBlank())
            ? ViewOutputFormat.fromString(format)
            : ViewOutputFormat.fromAcceptHeader(acceptHeader);
    final boolean shouldIncludeHeader = includeHeader == null || includeHeader.booleanValue();

    // Build the data source.
    final DataSource dataSource = buildDataSource(inlineResources, patientIds, groupIds, since);

    // Get column names from the view for CSV header.
    final List<String> columnNames = view.getAllColumns().map(Column::getName).toList();

    // Set response headers for streaming.
    response.setContentType(outputFormat.getContentType());
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    response.setStatus(HttpServletResponse.SC_OK);

    try {
      final OutputStream outputStream = response.getOutputStream();

      // For CSV with header, write header immediately to minimise TTFB.
      if (outputFormat == ViewOutputFormat.CSV && shouldIncludeHeader) {
        writeCsvHeader(outputStream, columnNames);
        outputStream.flush();
      }

      // Execute the view query and stream results.
      executeAndStreamResults(view, dataSource, limit, outputFormat, outputStream);

      outputStream.flush();
    } catch (final IOException e) {
      log.error("Error streaming view results", e);
      throw new InvalidRequestException("Error streaming results: " + e.getMessage());
    }
  }

  /** Executes the view query and streams results to the output stream. */
  private void executeAndStreamResults(
      @Nonnull final FhirView view,
      @Nonnull final DataSource dataSource,
      @Nullable final IntegerType limit,
      @Nonnull final ViewOutputFormat outputFormat,
      @Nonnull final OutputStream outputStream)
      throws IOException {

    final FhirViewExecutor executor =
        new FhirViewExecutor(fhirContext, dataSource, serverConfiguration.getQuery());
    Dataset<Row> result;
    try {
      result = executor.buildQuery(view);
    } catch (final ConstraintViolationException e) {
      throw new InvalidRequestException("Invalid ViewDefinition: " + e.getMessage());
    } catch (final UnsupportedOperationException e) {
      // Thrown when a FHIRPath expression is not supported, such as accessing a choice element
      // without specifying the type via ofType().
      throw new InvalidRequestException("Unsupported expression: " + e.getMessage());
    }

    // Apply limit if specified.
    if (limit != null && limit.getValue() != null) {
      result = result.limit(limit.getValue());
    }

    // Stream results.
    final StructType schema = result.schema();
    final Iterator<Row> iterator = result.toLocalIterator();

    switch (outputFormat) {
      case NDJSON -> streamNdjson(outputStream, iterator, schema);
      case JSON -> writeJson(outputStream, iterator, schema);
      default -> streamCsv(outputStream, iterator, schema);
    }
  }

  /**
   * Parses the ViewDefinition resource into a FhirView object.
   *
   * <p>This method serialises the HAPI resource back to JSON, then parses it with Gson into the
   * FhirView class. This approach avoids duplicating the FhirView class hierarchy as HAPI resource
   * components.
   */
  @Nonnull
  private FhirView parseViewDefinition(@Nonnull final IBaseResource viewResource) {
    try {
      // Serialise the HAPI resource back to JSON.
      final String viewJson = fhirContext.newJsonParser().encodeResourceToString(viewResource);
      // Parse the JSON into the FhirView class.
      return gson.fromJson(viewJson, FhirView.class);
    } catch (final JsonSyntaxException e) {
      throw new InvalidRequestException("Invalid ViewDefinition: " + e.getMessage());
    }
  }

  /** Builds the data source, either from inline resources or the Delta Lake. */
  @Nonnull
  private DataSource buildDataSource(
      @Nullable final List<String> inlineResources,
      @Nullable final List<String> patientIds,
      @Nullable final List<IdType> groupIds,
      @Nullable final InstantType since) {

    // If inline resources are provided, use them instead of Delta Lake.
    if (inlineResources != null && !inlineResources.isEmpty()) {
      final List<IBaseResource> resources = parseInlineResources(inlineResources);
      return new ObjectDataSource(sparkSession, fhirEncoders, resources);
    }

    // Otherwise use Delta Lake with filters.
    QueryableDataSource dataSource = deltaLake;

    // Apply _since filter.
    if (since != null) {
      dataSource =
          dataSource.map(
              rowDataset ->
                  rowDataset.filter(
                      "meta.lastUpdated IS NULL OR meta.lastUpdated >= '"
                          + since.getValueAsString()
                          + "'"));
    }

    // Collect all patient IDs from both patient and group parameters.
    final Set<String> allPatientIds = new HashSet<>();
    if (patientIds != null) {
      allPatientIds.addAll(patientIds);
    }
    if (groupIds != null) {
      for (final IdType groupId : groupIds) {
        allPatientIds.addAll(groupMemberService.extractPatientIdsFromGroup(groupId.getIdPart()));
      }
    }

    // Apply patient compartment filter if patient IDs were specified.
    if (!allPatientIds.isEmpty()) {
      dataSource = applyPatientCompartmentFilter(dataSource, allPatientIds);
    }

    return dataSource;
  }

  /** Parses inline FHIR resources from JSON strings. */
  @Nonnull
  private List<IBaseResource> parseInlineResources(@Nonnull final List<String> inlineResources) {
    final IParser jsonParser = fhirContext.newJsonParser();
    final List<IBaseResource> resources = new ArrayList<>();
    for (final String resourceJson : inlineResources) {
      try {
        resources.add(jsonParser.parseResource(resourceJson));
      } catch (final Exception e) {
        throw new InvalidRequestException("Invalid inline resource: " + e.getMessage());
      }
    }
    return resources;
  }

  /** Applies patient compartment filter to the data source. */
  @Nonnull
  private QueryableDataSource applyPatientCompartmentFilter(
      @Nonnull final QueryableDataSource dataSource, @Nonnull final Set<String> patientIds) {

    // Filter out resource types that are not in the Patient compartment.
    final QueryableDataSource filtered =
        dataSource.filterByResourceType(patientCompartmentService::isInPatientCompartment);

    // Apply row-level filtering based on patient compartment membership.
    return filtered.map(
        (resourceType, rowDataset) -> {
          final org.apache.spark.sql.Column patientFilter =
              patientCompartmentService.buildPatientFilter(resourceType, patientIds);
          log.debug(
              "Applying patient compartment filter for resource type {}: {}",
              resourceType,
              patientFilter);
          return rowDataset.filter(patientFilter);
        });
  }

  /** Writes the CSV header row. */
  private void writeCsvHeader(
      @Nonnull final OutputStream outputStream, @Nonnull final List<String> columnNames)
      throws IOException {
    final OutputStreamWriter writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
    final CSVPrinter printer =
        new CSVPrinter(
            writer,
            CSVFormat.DEFAULT
                .builder()
                .setHeader(columnNames.toArray(String[]::new))
                .setSkipHeaderRecord(false)
                .build());
    printer.flush();
    // Don't close the printer as we need to keep the stream open.
  }

  /** Streams results as NDJSON. */
  private void streamNdjson(
      @Nonnull final OutputStream outputStream,
      @Nonnull final Iterator<Row> iterator,
      @Nonnull final StructType schema)
      throws IOException {
    while (iterator.hasNext()) {
      final Row row = iterator.next();
      final String json = rowToJson(row, schema);
      outputStream.write(json.getBytes(StandardCharsets.UTF_8));
      outputStream.write('\n');
      outputStream.flush();
    }
  }

  /** Streams results as CSV. The header is written separately for TTFB optimisation. */
  private void streamCsv(
      @Nonnull final OutputStream outputStream,
      @Nonnull final Iterator<Row> iterator,
      @Nonnull final StructType schema)
      throws IOException {
    final OutputStreamWriter writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
    // Header was already written for TTFB optimisation, so always use DEFAULT (no header).
    final CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT);

    while (iterator.hasNext()) {
      final Row row = iterator.next();
      printer.printRecord(rowToList(row, schema));
      printer.flush();
    }
  }

  /**
   * Writes results as a single JSON document containing an array. Unlike NDJSON, this cannot be
   * streamed row-by-row as it requires proper JSON array formatting.
   */
  private void writeJson(
      @Nonnull final OutputStream outputStream,
      @Nonnull final Iterator<Row> iterator,
      @Nonnull final StructType schema)
      throws IOException {
    final List<Map<String, Object>> rows = new ArrayList<>();
    while (iterator.hasNext()) {
      final Row row = iterator.next();
      rows.add(rowToMap(row, schema));
    }
    final String json = gson.toJson(rows);
    outputStream.write(json.getBytes(StandardCharsets.UTF_8));
  }

  /** Converts a Spark Row to a Map for JSON serialisation. */
  @Nonnull
  private Map<String, Object> rowToMap(@Nonnull final Row row, @Nonnull final StructType schema) {
    final Map<String, Object> map = new LinkedHashMap<>();
    final StructField[] fields = schema.fields();
    for (int i = 0; i < fields.length; i++) {
      final String name = fields[i].name();
      final Object value = row.get(i);
      if (value != null) {
        map.put(name, convertValue(value, fields[i].dataType()));
      }
    }
    return map;
  }

  /** Converts a Spark Row to a JSON string. */
  @Nonnull
  private String rowToJson(@Nonnull final Row row, @Nonnull final StructType schema) {
    return gson.toJson(rowToMap(row, schema));
  }

  /** Converts a value for JSON serialisation. */
  @Nullable
  private Object convertValue(
      @Nullable final Object value, @Nonnull final org.apache.spark.sql.types.DataType dataType) {
    if (value == null) {
      return null;
    }
    // Handle nested struct.
    if (value instanceof final Row nestedRow && dataType instanceof final StructType structType) {
      final Map<String, Object> nested = new LinkedHashMap<>();
      final StructField[] fields = structType.fields();
      for (int i = 0; i < fields.length; i++) {
        final Object nestedValue = nestedRow.get(i);
        if (nestedValue != null) {
          nested.put(fields[i].name(), convertValue(nestedValue, fields[i].dataType()));
        }
      }
      return nested;
    }
    // Handle array.
    if (value instanceof final scala.collection.Seq<?> seq) {
      final List<?> list = CollectionConverters.asJava(seq);
      if (dataType instanceof final ArrayType arrayType) {
        return list.stream().map(item -> convertValue(item, arrayType.elementType())).toList();
      }
      return list;
    }
    return value;
  }

  /** Converts a Spark Row to a list of values for CSV output. */
  @Nonnull
  private List<Object> rowToList(@Nonnull final Row row, @Nonnull final StructType schema) {
    final List<Object> values = new ArrayList<>();
    final StructField[] fields = schema.fields();
    for (int i = 0; i < fields.length; i++) {
      final Object value = row.get(i);
      values.add(convertValueForCsv(value, fields[i].dataType()));
    }
    return values;
  }

  /** Converts a value for CSV output. */
  @Nullable
  private Object convertValueForCsv(
      @Nullable final Object value, @Nonnull final org.apache.spark.sql.types.DataType dataType) {
    if (value == null) {
      return null;
    }
    if (value instanceof Row || value instanceof scala.collection.Seq<?>) {
      // For complex types in CSV, serialise as JSON.
      return gson.toJson(convertValue(value, dataType));
    }
    return value;
  }
}
