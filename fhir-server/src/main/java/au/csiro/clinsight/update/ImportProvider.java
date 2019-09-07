/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.update;

import au.csiro.clinsight.fhir.AnalyticsServerConfiguration;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import com.cerner.bunsen.FhirEncoders;
import com.google.gson.Gson;
import java.io.IOException;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
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
  private final FhirContext fhirContext;
  private final FhirEncoders fhirEncoders;
  private final Gson gson;

  public ImportProvider(AnalyticsServerConfiguration configuration,
      SparkSession spark, FhirContext fhirContext, FhirEncoders fhirEncoders) {
    this.spark = spark;
    this.resourceWriter = new ResourceWriter(configuration.getWarehouseUrl(),
        configuration.getDatabaseName());
    this.fhirContext = fhirContext;
    this.fhirEncoders = fhirEncoders;
    gson = new Gson();
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
  @Operation(name = "$import", manualRequest = true)
  public OperationOutcome importOperation(HttpServletRequest request)
      throws IOException {
    String contentType = request.getHeader("Content-Type");
    if (!contentType.equals("application/json")) {
      throw new InvalidRequestException(
          "Request to $import operation must have Content-Type of application/json");
    }

    // Parse and validate the JSON request.
    ImportRequest importRequest = gson.fromJson(request.getReader(), ImportRequest.class);
    if (!importRequest.getInputFormat().equals("application/fhir+ndjson")) {
      throw new InvalidRequestException(
          "$import operation only supports inputFormat of application/fhir+ndjson");
    }

    // For each input within the request, read the resources of the declared type and create
    // the corresponding table in the warehouse.
    for (ImportRequestInput importRequestInput : importRequest.getInputs()) {
      String resourceName = importRequestInput.getType();
      ExpressionEncoder<IBaseResource> fhirEncoder = fhirEncoders.of(resourceName);
      @SuppressWarnings("UnnecessaryLocalVariable") FhirContext parsingContext = fhirContext;

      Dataset<String> jsonStrings = spark.read().textFile(importRequestInput.getUrl());
      Dataset resources = jsonStrings
          .map((MapFunction<String, IBaseResource>) json -> parsingContext.newJsonParser()
              .parseResource(json), fhirEncoder);

      logger.info("Saving resources: " + resourceName);
      resourceWriter.write(resourceName, resources);
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

  private class ImportRequest {

    private String inputFormat;
    private List<ImportRequestInput> inputs;

    public String getInputFormat() {
      return inputFormat;
    }

    public void setInputFormat(String inputFormat) {
      this.inputFormat = inputFormat;
    }

    public List<ImportRequestInput> getInputs() {
      return inputs;
    }

    public void setInputs(List<ImportRequestInput> inputs) {
      this.inputs = inputs;
    }
  }

  private class ImportRequestInput {

    private String type;
    private String url;

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public String getUrl() {
      return url;
    }

    public void setUrl(String url) {
      this.url = url;
    }
  }
}
