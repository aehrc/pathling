/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.update;

import au.csiro.clinsight.bunsen.FhirEncoders;
import au.csiro.clinsight.fhir.AnalyticsServerConfiguration;
import au.csiro.clinsight.fhir.FhirContextFactory;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.UrlType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enables the bulk import of data into the server.
 *
 * @author John Grimes
 */
public class ImportProvider {

  private static final Logger logger = LoggerFactory.getLogger(ImportProvider.class);
  private final SparkSession spark;
  private final ResourceWriter resourceWriter;
  private final FhirEncoders fhirEncoders;
  private final FhirContextFactory fhirContextFactory;

  public ImportProvider(AnalyticsServerConfiguration configuration, SparkSession spark,
      FhirEncoders fhirEncoders, FhirContextFactory fhirContextFactory) {
    this.spark = spark;
    this.resourceWriter = new ResourceWriter(configuration.getWarehouseUrl(),
        configuration.getDatabaseName());
    this.fhirEncoders = fhirEncoders;
    this.fhirContextFactory = fhirContextFactory;
  }

  /**
   * Accepts a request of type `application/fhir+ndjson` and overwrites the warehouse tables with
   * the contents. Does not currently support any sort of incremental update or appending to the
   * warehouse tables. Also does not currently support asynchronous processing.
   *
   * Each input will be treated as a file containing only one type of resource type. Bundles are not
   * currently given any special treatment. Each resource type is assumed to appear in the list only
   * once - multiple occurrences will result in the last input overwriting the previous ones.
   */
  @Operation(name = "$import")
  public OperationOutcome importOperation(@ResourceParam Parameters inParams) {
    // Parse and validate the JSON request.
    List<ParametersParameterComponent> sourceParams = inParams.getParameter().stream()
        .filter(param -> param.getName().equals("source")).collect(Collectors.toList());
    if (sourceParams.isEmpty()) {
      throw new InvalidRequestException("Must provide at least one source parameter");
    }

    // For each input within the request, read the resources of the declared type and create
    // the corresponding table in the warehouse.
    for (ParametersParameterComponent sourceParam : sourceParams) {
      ParametersParameterComponent resourceTypeParam = sourceParam.getPart().stream()
          .filter(param -> param.getName().equals("resourceType"))
          .findFirst()
          .orElseThrow(
              () -> new InvalidRequestException("Must provide resourceType for each source"));
      ParametersParameterComponent urlParam = sourceParam.getPart().stream()
          .filter(param -> param.getName().equals("url"))
          .findFirst()
          .orElseThrow(
              () -> new InvalidRequestException("Must provide url for each source"));

      String resourceCode = ((CodeType) resourceTypeParam.getValue()).getCode();
      ResourceType resourceType = ResourceType
          .fromCode(resourceCode);
      ExpressionEncoder<IBaseResource> fhirEncoder = fhirEncoders.of(resourceType.toCode());
      @SuppressWarnings("UnnecessaryLocalVariable") FhirContextFactory localFhirContextFactory = this.fhirContextFactory;

      String url = ((UrlType) urlParam.getValue()).getValueAsString();
      Dataset<String> jsonStrings = spark.read().textFile(url);
      Dataset resources = jsonStrings
          .map((MapFunction<String, IBaseResource>) json -> localFhirContextFactory
              .getFhirContext(FhirVersionEnum.R4).newJsonParser().parseResource(json), fhirEncoder);

      logger.info("Saving resources: " + resourceType.toCode());
      resourceWriter.write(resourceType, resources);
    }

    // We return 200, as this operation is currently synchronous.
    OperationOutcome opOutcome = new OperationOutcome();
    OperationOutcomeIssueComponent issue = new OperationOutcomeIssueComponent();
    issue.setSeverity(IssueSeverity.INFORMATION);
    issue.setCode(IssueType.INFORMATIONAL);
    issue.setDiagnostics("Data import completed successfully");
    opOutcome.getIssue().add(issue);
    return opOutcome;
  }

}
