/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

import ca.uhn.fhir.rest.annotation.Metadata;
import ca.uhn.fhir.rest.server.IServerConformanceProvider;
import ca.uhn.fhir.rest.server.RestfulServer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;
import org.hl7.fhir.dstu3.model.*;
import org.hl7.fhir.dstu3.model.CapabilityStatement.*;
import org.hl7.fhir.dstu3.model.Enumerations.PublicationStatus;

/**
 * This class provides a customised CapabilityStatement describing the functionality of the
 * analytics server.
 *
 * TODO: Merge advertised capabilities with those of any other servers that we proxy requests to,
 * such as the terminology server and FHIR REST server.
 *
 * @author John Grimes
 */
class AnalyticsServerCapabilities implements
    IServerConformanceProvider<CapabilityStatement> {

  @Override
  @Metadata
  public CapabilityStatement getServerConformance(HttpServletRequest httpServletRequest) {
    CapabilityStatement capabilityStatement = new CapabilityStatement();
    capabilityStatement
        .setUrl("https://clinsight.csiro.au/fhir/CapabilityStatement/clinsight-fhir-api-0");
    capabilityStatement.setVersion("0.0.0");
    capabilityStatement.setTitle("Clinsight FHIR API");
    capabilityStatement.setName("clinsight-fhir-api");
    capabilityStatement.setStatus(PublicationStatus.DRAFT);
    capabilityStatement.setExperimental(true);
    capabilityStatement.setPublisher("Australian e-Health Research Centre, CSIRO");
    capabilityStatement.setCopyright(
        "Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.");
    capabilityStatement.setUseContext(buildUseContext());
    capabilityStatement.setKind(CapabilityStatementKind.CAPABILITY);
    capabilityStatement.setSoftware(
        new CapabilityStatementSoftwareComponent(new StringType("Clinsight FHIR Server")));
    capabilityStatement.setFhirVersion("3.0.1");
    capabilityStatement.setAcceptUnknown(UnknownContentCode.NO);
    capabilityStatement.setFormat(Arrays.asList(new CodeType("json"), new CodeType("xml")));
    capabilityStatement.setRest(buildRestComponent());
    return capabilityStatement;
  }

  @Nonnull
  private List<UsageContext> buildUseContext() {
    List<UsageContext> useContext = new ArrayList<>();
    CodeableConcept task = new CodeableConcept();
    task.setText("Aggregate data analytics");
    Coding usageContextType = new Coding("http://hl7.org/fhir/usage-context-type", "task",
        "Workflow Task");
    useContext.add(new UsageContext(usageContextType, task));
    return useContext;
  }

  @Nonnull
  private List<CapabilityStatementRestComponent> buildRestComponent() {
    List<CapabilityStatementRestComponent> rest = new ArrayList<>();
    CapabilityStatementRestComponent server = new CapabilityStatementRestComponent();
    server.setMode(RestfulCapabilityMode.SERVER);
    List<CapabilityStatementRestOperationComponent> operations = new ArrayList<>();
    Reference queryOperationReference = new Reference("OperationDefinition/aggregate-query-0");
    CapabilityStatementRestOperationComponent operation = new CapabilityStatementRestOperationComponent(
        new StringType("aggregate-query"), queryOperationReference);
    operations.add(operation);
    server.setOperation(operations);
    rest.add(server);
    return rest;
  }

  @Override
  public void setRestfulServer(RestfulServer restfulServer) {
  }

}
