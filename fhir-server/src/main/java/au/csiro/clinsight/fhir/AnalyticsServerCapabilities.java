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
import javax.servlet.http.HttpServletRequest;
import org.hl7.fhir.dstu3.model.CapabilityStatement;
import org.hl7.fhir.dstu3.model.CapabilityStatement.CapabilityStatementKind;
import org.hl7.fhir.dstu3.model.CapabilityStatement.CapabilityStatementRestComponent;
import org.hl7.fhir.dstu3.model.CapabilityStatement.CapabilityStatementRestOperationComponent;
import org.hl7.fhir.dstu3.model.CapabilityStatement.CapabilityStatementRestResourceComponent;
import org.hl7.fhir.dstu3.model.CapabilityStatement.CapabilityStatementSoftwareComponent;
import org.hl7.fhir.dstu3.model.CapabilityStatement.RestfulCapabilityMode;
import org.hl7.fhir.dstu3.model.CapabilityStatement.UnknownContentCode;
import org.hl7.fhir.dstu3.model.CodeType;
import org.hl7.fhir.dstu3.model.CodeableConcept;
import org.hl7.fhir.dstu3.model.Coding;
import org.hl7.fhir.dstu3.model.Enumerations.PublicationStatus;
import org.hl7.fhir.dstu3.model.Reference;
import org.hl7.fhir.dstu3.model.StringType;
import org.hl7.fhir.dstu3.model.UsageContext;

/**
 * @author John Grimes
 */
public class AnalyticsServerCapabilities implements
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
    List<UsageContext> useContext = new ArrayList<>();
    CodeableConcept task = new CodeableConcept();
    task.setText("Aggregate data analytics");
    Coding usageContextType = new Coding("http://hl7.org/fhir/usage-context-type", "task",
        "Workflow Task");
    useContext.add(new UsageContext(usageContextType, task));
    capabilityStatement.setUseContext(useContext);
    capabilityStatement.setCopyright(
        "Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.");
    capabilityStatement.setKind(CapabilityStatementKind.CAPABILITY);
    capabilityStatement.setSoftware(
        new CapabilityStatementSoftwareComponent(new StringType("Clinsight FHIR Server")));
    capabilityStatement.setFhirVersion("3.0.1");
    capabilityStatement.setAcceptUnknown(UnknownContentCode.NO);
    capabilityStatement.setFormat(Arrays.asList(new CodeType("json"), new CodeType("xml")));
    Reference aggregateQueryReference = new Reference("StructureDefinition/AggregateQuery-0");
    Reference aggregateQueryResultReference = new Reference(
        "StructureDefinition/AggregateQueryResult-0");
    capabilityStatement
        .setProfile(Arrays.asList(aggregateQueryReference, aggregateQueryResultReference));
    List<CapabilityStatementRestComponent> rest = new ArrayList<>();
    CapabilityStatementRestComponent server = new CapabilityStatementRestComponent();
    server.setMode(RestfulCapabilityMode.SERVER);
    List<CapabilityStatementRestResourceComponent> resources = new ArrayList<>();
    CapabilityStatementRestResourceComponent queryResource = new CapabilityStatementRestResourceComponent();
    queryResource.setType("Basic");
    queryResource.setProfile(aggregateQueryReference);
    server.setResource(resources);
    List<CapabilityStatementRestOperationComponent> operations = new ArrayList<>();
    Reference queryOperationReference = new Reference("OperationDefinition/aggregateQuery-0");
    CapabilityStatementRestOperationComponent operation = new CapabilityStatementRestOperationComponent(
        new StringType("aggregateQuery"), queryOperationReference);
    operations.add(operation);
    server.setOperation(operations);
    rest.add(server);
    capabilityStatement.setRest(rest);
    return capabilityStatement;
  }

  @Override
  public void setRestfulServer(RestfulServer restfulServer) {
  }

}
