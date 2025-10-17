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

package au.csiro.pathling.operations.import_;

import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.UnsupportedResourceError;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.errors.SecurityError;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.sink.DataSinkBuilder;
import au.csiro.pathling.library.io.source.DataSourceBuilder;
import au.csiro.pathling.library.io.source.DatasetSource;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

/**
 * Encapsulates the execution of an import operation.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/server/operations/import">Import</a>
 */
@Component
@Profile({"core", "import"})
@Slf4j
public class ImportExecutor {

  @Nonnull
  private final SparkSession spark;

  @Nonnull
  private final FhirEncoders fhirEncoders;

  @Nonnull
  private final Optional<AccessRules> accessRules;
  private final PathlingContext pathlingContext;

  /**
   * @param spark a {@link SparkSession} for resolving Spark queries
   * @param database a {@link Database} for writing resources
   * @param fhirEncoders a {@link FhirEncoders} object for converting data back into HAPI FHIR
   * @param fhirContextFactory a {@link FhirContextFactory} for constructing FhirContext objects in
   * the context of parallel processing
   * @param accessRules a {@link AccessRules} for validating access to URLs
   */
  public ImportExecutor(@Nonnull final SparkSession spark,
                        @Nonnull final FhirEncoders fhirEncoders,
                        @Nonnull final Optional<AccessRules> accessRules,
      PathlingContext pathlingContext) {
    this.spark = spark;
    this.fhirEncoders = fhirEncoders;
    this.accessRules = accessRules;
    this.pathlingContext = pathlingContext;
  }
  
  private record ImportSource(String fileUrl, ImportMode importMode) {}
  
  /**
   * Executes an import request.
   *
   * @param inParams a FHIR {@link Parameters} object describing the import request
   * @return a FHIR {@link OperationOutcome} resource describing the result
   */
  @Nonnull
  public OperationOutcome execute(@Nonnull @ResourceParam final Parameters inParams) {
    // Parse and validate the JSON request.
    final List<ParametersParameterComponent> inputParams = inParams.getParameter().stream()
        .filter(param -> "input".equals(param.getName())).toList();
    if (inputParams.isEmpty()) {
      throw new InvalidUserInputError("Must provide at least one input parameter");
    }
    log.info("Received $import request");
    
    /*
    source [1..*] - A source FHIR NDJSON file containing resources to be included within this import operation. Each file must contain only one type of resource.
        resourceType [1..1] (code) - The base FHIR resource type contained within this source file. Code must be a member of http://hl7.org/fhir/ValueSet/resource-types.
        url [1..1] (uri) - A URL that can be used to retrieve this source file.

    mode [0..1] (code) - A value of overwrite will cause all existing resources of the specified type to be deleted and replaced with the contents of the source file. A value of merge will match existing resources with updated resources in the source file based on their ID, and either update the existing resources or add new resources as appropriate. The default value is overwrite.
    format [0..1] (code) - Indicates the format of the source file. Possible values are ndjson, parquet and delta. The default value is ndjson.
     */
    
    List<ParametersParameterComponent> sourceParams = new ArrayList<>();
    
    // For each input within the request, read the resources of the declared type and create
    // the corresponding table in the warehouse.
    for (final ParametersParameterComponent sourceParam : sourceParams) {
      final ParametersParameterComponent resourceTypeParam = sourceParam.getPart().stream()
          .filter(param -> "resourceType".equals(param.getName()))
          .findFirst()
          .orElseThrow(
              () -> new InvalidUserInputError("Must provide resourceType for each source"));
      final ParametersParameterComponent urlParam = sourceParam.getPart().stream()
          .filter(param -> "url".equals(param.getName()))
          .findFirst()
          .orElseThrow(
              () -> new InvalidUserInputError("Must provide url for each source"));
      // The mode parameter defaults to 'overwrite'.
      final ImportMode importMode = sourceParam.getPart().stream()
          .filter(param -> "mode".equals(param.getName()) &&
              param.getValue() instanceof CodeType)
          .findFirst()
          .map(param -> ImportMode.fromCode(
              ((CodeType) param.getValue()).asStringValue()))
          .orElse(ImportMode.OVERWRITE);

      // Get the serialized resource type from the source parameter.
      final ImportFormat format = sourceParam.getPart().stream()
          .filter(param -> "format".equals(param.getName()))
          .findFirst()
          .map(param -> {
            final String formatCode = ((StringType) param.getValue()).getValue();
            try {
              return ImportFormat.fromCode(formatCode);
            } catch (final IllegalArgumentException e) {
              throw new InvalidUserInputError("Unsupported format: " + formatCode);
            }
          })
          .orElse(ImportFormat.NDJSON);

      final String resourceCode = ((CodeType) resourceTypeParam.getValue()).getCode();
      final ResourceType resourceType = ResourceType.fromCode(resourceCode);

      // Get an encoder based on the declared resource type within the source parameter.
      final ExpressionEncoder<IBaseResource> fhirEncoder;
      try {
        fhirEncoder = fhirEncoders.of(resourceType.toCode());
      } catch (final UnsupportedResourceError e) {
        throw new InvalidUserInputError("Unsupported resource type: " + resourceCode);
      }

      // Read the resources from the source URL into a dataset of strings.
      final QueryableDataSource queryableDataSource = readRowsFromUrl(urlParam, format, fhirEncoder);

      log.info("Importing {} resources (mode: {})", resourceType.toCode(), importMode.getCode());
      DataSinkBuilder sinkBuilder = new DataSinkBuilder(pathlingContext, queryableDataSource).saveMode(importMode.getCode());
      // switch (format) {
      //   case NDJSON -> sinkBuilder.ndjson()
      // }
      // if (importMode == ImportMode.OVERWRITE) {
      //  
      //   database.overwrite(resourceType, rows);
      // } else {
      //   database.merge(resourceType, rows);
      // }
    }

    // We return 200, as this operation is currently synchronous.
    log.info("Import complete");

    // Construct a response.
    final OperationOutcome opOutcome = new OperationOutcome();
    final OperationOutcomeIssueComponent issue = new OperationOutcomeIssueComponent();
    issue.setSeverity(IssueSeverity.INFORMATION);
    issue.setCode(IssueType.INFORMATIONAL);
    issue.setDiagnostics("Data import completed successfully");
    opOutcome.getIssue().add(issue);
    return opOutcome;
  }

  @Nonnull
  private QueryableDataSource readRowsFromUrl(@Nonnull final ParametersParameterComponent urlParam,
      final ImportFormat format, final ExpressionEncoder<IBaseResource> fhirEncoder) {
    final String url = ((UrlType) urlParam.getValue()).getValueAsString();
    final String decodedUrl = URLDecoder.decode(url, StandardCharsets.UTF_8);
    final String convertedUrl = CacheableDatabase.convertS3ToS3aUrl(decodedUrl);
    try {
      // Check that the user is authorized to execute the operation.
      accessRules.ifPresent(ar -> ar.checkCanImportFrom(convertedUrl));
      final FilterFunction<String> nonBlanks = s -> !s.isBlank();

      DataSourceBuilder sourceBuilder = new DataSourceBuilder(pathlingContext);
      return switch (format) {
        case NDJSON -> sourceBuilder.ndjson(convertedUrl);
        case PARQUET -> sourceBuilder.parquet(convertedUrl);
        case DELTA -> sourceBuilder.delta(convertedUrl);
      };
    } catch (final SecurityError e) {
      throw new InvalidUserInputError("Not allowed to import from URL: " + convertedUrl, e);
    } catch (final Exception e) {
      throw new InvalidUserInputError("Error reading from URL: " + convertedUrl, e);
    }
  }

}
