/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.compartment.PatientCompartmentService;
import au.csiro.pathling.security.OperationAccess;
import au.csiro.pathling.views.Column;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewExecutor;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.validation.ConstraintViolationException;
import jakarta.servlet.http.HttpServletResponse;
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
 * Provider for the $viewdefinition-run operation from the SQL on FHIR specification.
 *
 * @author John Grimes
 * @see <a
 * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/OperationDefinition-ViewDefinitionRun.html">ViewDefinitionRun</a>
 */
@Slf4j
@Component
public class ViewDefinitionRunProvider {

  private static final String PATIENT_REFERENCE_PREFIX = "Patient/";

  @Nonnull
  private final SparkSession sparkSession;

  @Nonnull
  private final QueryableDataSource deltaLake;

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final FhirEncoders fhirEncoders;

  @Nonnull
  private final PatientCompartmentService patientCompartmentService;

  @Nonnull
  private final ServerConfiguration serverConfiguration;

  @Nonnull
  private final Gson gson;

  /**
   * Constructs a new ViewDefinitionRunProvider.
   *
   * @param sparkSession the Spark session
   * @param deltaLake the queryable data source
   * @param fhirContext the FHIR context
   * @param fhirEncoders the FHIR encoders
   * @param patientCompartmentService the patient compartment service
   * @param serverConfiguration the server configuration
   */
  @Autowired
  public ViewDefinitionRunProvider(
      @Nonnull final SparkSession sparkSession,
      @Nonnull final QueryableDataSource deltaLake,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final PatientCompartmentService patientCompartmentService,
      @Nonnull final ServerConfiguration serverConfiguration) {
    this.sparkSession = sparkSession;
    this.deltaLake = deltaLake;
    this.fhirContext = fhirContext;
    this.fhirEncoders = fhirEncoders;
    this.patientCompartmentService = patientCompartmentService;
    this.serverConfiguration = serverConfiguration;
    this.gson = buildGson();
  }

  /**
   * Builds a Gson instance for ViewDefinition parsing and JSON output.
   */
  @Nonnull
  private static Gson buildGson() {
    return new GsonBuilder().create();
  }

  /**
   * Executes a ViewDefinition and returns the results in NDJSON or CSV format with chunked
   * streaming.
   *
   * @param viewResource the ViewDefinition resource
   * @param format the output format (ndjson or csv)
   * @param includeHeader whether to include a header row in CSV output
   * @param limit the maximum number of rows to return
   * @param patientIds patient IDs to filter by
   * @param groupIds group IDs to filter by
   * @param since filter by meta.lastUpdated >= value
   * @param inlineResources FHIR resources to use instead of the main data source
   * @param response the HTTP response for streaming output
   */
  // Suppress parameter count warning - HAPI FHIR operation parameters are defined by the spec.
  @SuppressWarnings("java:S107")
  @Operation(name = "$viewdefinition-run", idempotent = true, manualResponse = true)
  @OperationAccess("view-run")
  public void run(
      @Nonnull @OperationParam(name = "viewResource") final IBaseResource viewResource,
      @Nullable @OperationParam(name = "_format") final String format,
      @Nullable @OperationParam(name = "header") final BooleanType includeHeader,
      @Nullable @OperationParam(name = "_limit") final IntegerType limit,
      @Nullable @OperationParam(name = "patient") final List<String> patientIds,
      @Nullable @OperationParam(name = "group") final List<IdType> groupIds,
      @Nullable @OperationParam(name = "_since") final InstantType since,
      @Nullable @OperationParam(name = "resource") final List<String> inlineResources,
      @Nullable final HttpServletResponse response) {

    if (response == null) {
      throw new InvalidRequestException("HTTP response is required for this operation");
    }

    // Parse the ViewDefinition.
    final FhirView view = parseViewDefinition(viewResource);

    // Determine output format.
    final ViewOutputFormat outputFormat = ViewOutputFormat.fromString(format);
    final boolean shouldIncludeHeader = includeHeader == null || includeHeader.booleanValue();

    // Build the data source.
    final DataSource dataSource = buildDataSource(inlineResources, patientIds, groupIds, since);

    // Get column names from the view for CSV header.
    final List<String> columnNames = view.getAllColumns()
        .map(Column::getName)
        .toList();

    // Set response headers for streaming.
    response.setContentType(outputFormat.getContentType());
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    response.setStatus(HttpServletResponse.SC_OK);

    try {
      final OutputStream outputStream = response.getOutputStream();

      // For CSV with header, write header immediately to minimise TTFB.
      if (outputFormat == ViewOutputFormat.CSV && shouldIncludeHeader) {
        writeCSVHeader(outputStream, columnNames);
        outputStream.flush();
      }

      // Execute the view query.
      final FhirViewExecutor executor = new FhirViewExecutor(
          fhirContext, sparkSession, dataSource, serverConfiguration.getQuery());
      Dataset<Row> result;
      try {
        result = executor.buildQuery(view);
      } catch (final ConstraintViolationException e) {
        throw new InvalidRequestException("Invalid ViewDefinition: " + e.getMessage());
      }

      // Apply limit if specified.
      if (limit != null && limit.getValue() != null) {
        result = result.limit(limit.getValue());
      }

      // Stream results.
      final StructType schema = result.schema();
      final Iterator<Row> iterator = result.toLocalIterator();

      if (outputFormat == ViewOutputFormat.NDJSON) {
        streamNdjson(outputStream, iterator, schema);
      } else {
        streamCSV(outputStream, iterator, schema);
      }

      outputStream.flush();
    } catch (final IOException e) {
      log.error("Error streaming view results", e);
      throw new InvalidRequestException("Error streaming results: " + e.getMessage());
    }
  }

  /**
   * Parses the ViewDefinition resource into a FhirView object.
   * <p>
   * This method serialises the HAPI resource back to JSON, then parses it with Gson into the
   * FhirView class. This approach avoids duplicating the FhirView class hierarchy as HAPI resource
   * components.
   * </p>
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

  /**
   * Builds the data source, either from inline resources or the Delta Lake.
   */
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
      dataSource = dataSource.map(rowDataset -> rowDataset.filter(
          "meta.lastUpdated IS NULL OR meta.lastUpdated >= '" + since.getValueAsString() + "'"));
    }

    // Collect all patient IDs from both patient and group parameters.
    final Set<String> allPatientIds = new HashSet<>();
    if (patientIds != null) {
      allPatientIds.addAll(patientIds);
    }
    if (groupIds != null) {
      for (final IdType groupId : groupIds) {
        allPatientIds.addAll(extractPatientIdsFromGroup(groupId.getIdPart()));
      }
    }

    // Apply patient compartment filter if patient IDs were specified.
    if (!allPatientIds.isEmpty()) {
      dataSource = applyPatientCompartmentFilter(dataSource, allPatientIds);
    }

    return dataSource;
  }

  /**
   * Parses inline FHIR resources from JSON strings.
   */
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

  /**
   * Extracts patient IDs from a Group resource.
   */
  @Nonnull
  private Set<String> extractPatientIdsFromGroup(@Nonnull final String groupId) {
    final Dataset<Row> groupDataset = deltaLake.read("Group");
    final Dataset<Row> filtered = groupDataset.filter(col("id").equalTo(groupId));

    if (filtered.isEmpty()) {
      throw new ResourceNotFoundException("Group/" + groupId);
    }

    final Dataset<Row> memberRefs = filtered
        .select(explode(col("member")).as("member"))
        .select(col("member.entity.reference").as("reference"));

    final Set<String> patientIds = new HashSet<>();
    for (final Row row : memberRefs.collectAsList()) {
      final String reference = row.getString(0);
      if (reference != null && reference.startsWith(PATIENT_REFERENCE_PREFIX)) {
        patientIds.add(reference.substring(PATIENT_REFERENCE_PREFIX.length()));
      }
    }

    log.debug("Extracted {} patient IDs from Group/{}", patientIds.size(), groupId);
    return patientIds;
  }

  /**
   * Applies patient compartment filter to the data source.
   */
  @Nonnull
  private QueryableDataSource applyPatientCompartmentFilter(
      @Nonnull final QueryableDataSource dataSource,
      @Nonnull final Set<String> patientIds) {

    // Filter out resource types that are not in the Patient compartment.
    final QueryableDataSource filtered = dataSource.filterByResourceType(
        patientCompartmentService::isInPatientCompartment);

    // Apply row-level filtering based on patient compartment membership.
    return filtered.map((resourceType, rowDataset) -> {
      final org.apache.spark.sql.Column patientFilter =
          patientCompartmentService.buildPatientFilter(resourceType, patientIds);
      log.debug("Applying patient compartment filter for resource type {}: {}",
          resourceType, patientFilter);
      return rowDataset.filter(patientFilter);
    });
  }

  /**
   * Writes the CSV header row.
   */
  private void writeCSVHeader(@Nonnull final OutputStream outputStream,
      @Nonnull final List<String> columnNames) throws IOException {
    final OutputStreamWriter writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
    final CSVPrinter printer = new CSVPrinter(writer,
        CSVFormat.DEFAULT.builder()
            .setHeader(columnNames.toArray(String[]::new))
            .setSkipHeaderRecord(false)
            .build());
    printer.flush();
    // Don't close the printer as we need to keep the stream open.
  }

  /**
   * Streams results as NDJSON.
   */
  private void streamNdjson(@Nonnull final OutputStream outputStream,
      @Nonnull final Iterator<Row> iterator,
      @Nonnull final StructType schema) throws IOException {
    while (iterator.hasNext()) {
      final Row row = iterator.next();
      final String json = rowToJson(row, schema);
      outputStream.write(json.getBytes(StandardCharsets.UTF_8));
      outputStream.write('\n');
      outputStream.flush();
    }
  }

  /**
   * Streams results as CSV. The header is written separately for TTFB optimisation.
   *
   * @param outputStream the output stream to write to
   * @param iterator the row iterator
   * @param schema the result schema
   */
  private void streamCSV(@Nonnull final OutputStream outputStream,
      @Nonnull final Iterator<Row> iterator,
      @Nonnull final StructType schema) throws IOException {
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
   * Converts a Spark Row to a JSON string.
   */
  @Nonnull
  private String rowToJson(@Nonnull final Row row, @Nonnull final StructType schema) {
    final Map<String, Object> map = new LinkedHashMap<>();
    final StructField[] fields = schema.fields();
    for (int i = 0; i < fields.length; i++) {
      final String name = fields[i].name();
      final Object value = row.get(i);
      if (value != null) {
        map.put(name, convertValue(value, fields[i].dataType()));
      }
    }
    return gson.toJson(map);
  }

  /**
   * Converts a value for JSON serialisation.
   */
  @Nullable
  private Object convertValue(@Nullable final Object value,
      @Nonnull final org.apache.spark.sql.types.DataType dataType) {
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
        return list.stream()
            .map(item -> convertValue(item, arrayType.elementType()))
            .toList();
      }
      return list;
    }
    return value;
  }

  /**
   * Converts a Spark Row to a list of values for CSV output.
   */
  @Nonnull
  private List<Object> rowToList(@Nonnull final Row row, @Nonnull final StructType schema) {
    final List<Object> values = new ArrayList<>();
    final StructField[] fields = schema.fields();
    for (int i = 0; i < fields.length; i++) {
      final Object value = row.get(i);
      values.add(convertValueForCSV(value, fields[i].dataType()));
    }
    return values;
  }

  /**
   * Converts a value for CSV output.
   */
  @Nullable
  private Object convertValueForCSV(@Nullable final Object value,
      @Nonnull final org.apache.spark.sql.types.DataType dataType) {
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
