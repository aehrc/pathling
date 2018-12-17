/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.transformers;

import au.csiro.clinsight.TerminologyClient;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.DataTypeException;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.v24.datatype.PL;
import ca.uhn.hl7v2.model.v24.group.ORU_R01_OBSERVATION;
import ca.uhn.hl7v2.model.v24.group.ORU_R01_ORDER_OBSERVATION;
import ca.uhn.hl7v2.model.v24.group.ORU_R01_PATIENT_RESULT;
import ca.uhn.hl7v2.model.v24.message.ORU_R01;
import ca.uhn.hl7v2.model.v24.segment.OBR;
import ca.uhn.hl7v2.model.v24.segment.OBX;
import ca.uhn.hl7v2.model.v24.segment.PV1;
import ca.uhn.hl7v2.parser.Parser;
import org.apache.commons.codec.digest.DigestUtils;
import org.hl7.fhir.dstu3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author John Grimes
 * @author Ming Zhang
 */
public class EpcarToFhirTransformer {

    private static final Logger logger = LoggerFactory.getLogger(EpcarToFhirTransformer.class);
    private static final String V2_PATIENT_CLASS_CS = "http://hl7.org.au/fhir/v2.4/0004";
    private static final String V2_PATIENT_CLASS_VS = "http://hl7.org.au/fhir/ValueSet/v2.4/0004";
    private static final String FHIR_ACT_ENCOUNTER_VS = "http://hl7.org/fhir/v3/ActCode";
    private static final String NAMESPACE_PREFIX = "https://clinsight.csiro.au/fhir/CodeSystem";
    private static final String NS_PROCEDURE_REQUEST_ID = NAMESPACE_PREFIX + "/epcar-procedure-request-id";
    private static final String NS_DIAGNOSTIC_REPORT_ID = NAMESPACE_PREFIX + "/epcar-diagnostic-report-id";
    private static final String NS_OBSERVATION_ID = NAMESPACE_PREFIX + "/epcar-observation-id";
    private static Map<String, String> v2CodeSystemToUri;

    static {
        v2CodeSystemToUri = Map.of(
                "PLS", NAMESPACE_PREFIX + "/epcar-observation-code-pls",
                "LN", "http://loinc.org"
        );
    }

    private HapiContext hapiContext;
    private TerminologyClient terminologyClient;

    public EpcarToFhirTransformer(HapiContext hapiContext, TerminologyClient terminologyClient) {
        this.hapiContext = hapiContext;
        this.terminologyClient = terminologyClient;
    }

    private static String getUriForV2CodeSystem(String system) {
        if (system == null) return null;
        return v2CodeSystemToUri.get(system);
    }

    private static void addPutResourceEntry(Bundle bundle, Resource resource) {
        Bundle.BundleEntryComponent entry = new Bundle.BundleEntryComponent();
        entry.setResource(resource);
        Bundle.BundleEntryRequestComponent request = new Bundle.BundleEntryRequestComponent();
        request.setUrl(resource.fhirType() + "/" + resource.getIdElement().getIdPart());
        request.setMethod(Bundle.HTTPVerb.PUT);
        entry.setRequest(request);
        bundle.addEntry(entry);
    }

    /**
     * Takes a HL7 v2.4 (AU) message as a string, and returns an equivalent FHIR Bundle.
     */
    public Bundle transform(String message) throws Exception {
        long start = System.nanoTime();

        ORU_R01 parsedMessage = (ORU_R01) parseHl7Message(message);
        Bundle bundle = new Bundle();
        bundle.setType(Bundle.BundleType.TRANSACTION);

        for (ORU_R01_PATIENT_RESULT patientResult : parsedMessage.getPATIENT_RESULTAll()) {
            transformPatientResult(bundle, patientResult);
        }

        double elapsedSecs = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        logger.info("Converted HL7 v2.4 pathology report in " + String.format("%.1f", elapsedSecs) + " ms");
        return bundle;
    }

    private Message parseHl7Message(String message) throws HL7Exception {
        Parser hl7Parser = hapiContext.getGenericParser();
        return hl7Parser.parse(message);
    }

    private void transformPatientResult(Bundle bundle, ORU_R01_PATIENT_RESULT patientResult)
            throws Exception {
        // Extract Encounter resource.
        PV1 pv1 = patientResult.getPATIENT().getVISIT().getPV1();
        Encounter encounter = extractEncounter(pv1);

        // Transform all Order Observations within message.
        List<ORU_R01_ORDER_OBSERVATION> orderObservationAll = patientResult.getORDER_OBSERVATIONAll();
        for (ORU_R01_ORDER_OBSERVATION orderObservation : orderObservationAll) {
            transformOrderObservation(bundle, encounter, orderObservation);
        }
    }

    private void transformOrderObservation(Bundle bundle, Encounter encounter,
                                           ORU_R01_ORDER_OBSERVATION orderObservation) throws Exception {
        OBR obr = orderObservation.getOBR();

        // Create ProcedureRequest and DiagnosticReport resources.
        ProcedureRequest procedureRequest = extractProcedureRequest(obr);
        DiagnosticReport diagnosticReport = extractDiagnosticReport(obr);
        Optional<Identifier> diagnosticReportIdentifier =
                diagnosticReport.getIdentifier().stream().filter(id -> id.getSystem()
                                                                         .equals(NS_DIAGNOSTIC_REPORT_ID)).findFirst();
        if (!diagnosticReportIdentifier.isPresent()) throw new Exception("Unable to retrieve identifier from " +
                                                                                 "DiagnosticReport");

        // Parse observations and add Observation resources to Bundle.
        List<ORU_R01_OBSERVATION> observationAll = orderObservation.getOBSERVATIONAll();
        for (ORU_R01_OBSERVATION observation : observationAll) {
            OBX obx = observation.getOBX();
            Observation fhirObservation = extractObservation(obx, diagnosticReportIdentifier.get().getValue());

            // Add a `basedOn` reference to the Observation, pointing back to the ProcedureRequest.
            Reference observationBasedOn = new Reference();
            observationBasedOn.setResource(procedureRequest);
            fhirObservation.getBasedOn().add(observationBasedOn);

            // Add a reference to this Observation to the DiagnosticReport.
            Reference observationReference = new Reference();
            observationReference.setResource(fhirObservation);
            diagnosticReport.getResult().add(observationReference);

            addPutResourceEntry(bundle, fhirObservation);
        }

        // Create associations between resources.
        procedureRequest.getContext().setResource(encounter);
        Reference basedOnReference = new Reference();
        basedOnReference.setResource(procedureRequest);
        diagnosticReport.getBasedOn().add(basedOnReference);

        // Add resources to Bundle.
        addPutResourceEntry(bundle, procedureRequest);
        addPutResourceEntry(bundle, diagnosticReport);
    }

    /**
     * Processes the Patient Visit (PV1) part of the HL7 v2 message, and returns an Encounter resource.
     */
    private Encounter extractEncounter(PV1 pv1) throws HL7Exception {
        Encounter encounter = new Encounter();

        // Populate class of Encounter.
        String patientClassCode = pv1.getPatientClass().getValue();
        Parameters translateResult =
                terminologyClient.translate(new CodeType(patientClassCode),
                                            new UriType(V2_PATIENT_CLASS_CS),
                                            new UriType(V2_PATIENT_CLASS_VS),
                                            new UriType(
                                                    FHIR_ACT_ENCOUNTER_VS));
        Optional<Parameters.ParametersParameterComponent> result =
                translateResult.getParameter().stream().filter(param -> param.getName().equals("result")).findFirst();
        boolean resultValue =
                result.isPresent() && result.get().getValue() instanceof BooleanType &&
                        ((BooleanType) result.get().getValue()).booleanValue();
        List<Parameters.ParametersParameterComponent> matches =
                translateResult.getParameter()
                               .stream()
                               .filter(param -> param.getName()
                                                     .equals("match"))
                               .collect(Collectors.toList());
        if (resultValue) {
            Optional<Parameters.ParametersParameterComponent> selectedMatch = selectTranslation(matches);
            if (selectedMatch.isPresent()) {
                Optional<Parameters.ParametersParameterComponent> concept =
                        selectedMatch.get()
                                     .getPart()
                                     .stream()
                                     .filter(part -> part.getName().equals("concept"))
                                     .findFirst();
                if (concept.isPresent() && concept.get().getValue() instanceof Coding)
                    encounter.setClass_((Coding) concept.get().getValue());
                else logger.warn("Unable to get concept value from match parameter in translate response");
            } else logger.warn("Unable to select match from translate response");
        } else logger.warn("Result parameter not found within translate response");

        // Populate assigned patient location.
        PL pl = pv1.getAssignedPatientLocation();
        String pointOfCare = pl.getPointOfCare().getValue();
        String facility = pl.getFacility().encode();
        Location pointOfCareLocation = new Location();
        Location facilityLocation = new Location();
        pointOfCareLocation.setName(pointOfCare);
        facilityLocation.setName(facility);
        // Point of care is modelled as a part of the facility Location.
        pointOfCareLocation.getPartOf().setResource(facilityLocation);
        Reference pointOfCareLocationReference = new Reference();
        pointOfCareLocationReference.setResource(pointOfCareLocation);
        encounter.getLocation().add(new Encounter.EncounterLocationComponent(pointOfCareLocationReference));

        return encounter;
    }

    /**
     * Processes the Observation Request (OBR) part of the HL7 v2 message, and returns a ProcedureRequest resource.
     */
    private ProcedureRequest extractProcedureRequest(OBR obr) throws DataTypeException {
        ProcedureRequest procedureRequest = new ProcedureRequest();
        String fillerOrderNumber = obr.getFillerOrderNumber().getEntityIdentifier().toString();

        // Create a logical ID using a hashed version of the filler order number.
        String id = getHashedId(NS_PROCEDURE_REQUEST_ID, fillerOrderNumber);
        procedureRequest.setId(id);

        // Create an identifier using a hashed version of the filler order number.
        Identifier identifier = getHashedIdentifier(NS_PROCEDURE_REQUEST_ID, fillerOrderNumber);
        procedureRequest.getIdentifier().add(identifier);

        // Add the coding to represent the type of test requested.
        String usiIdentifier = obr.getUniversalServiceIdentifier().getIdentifier().toString();
        String usiText = obr.getUniversalServiceIdentifier().getText().toString();
        String usiSystem = obr.getUniversalServiceIdentifier().getNameOfCodingSystem().toString();
        Coding codeCoding = new Coding();
        codeCoding.setSystem(getUriForV2CodeSystem(usiSystem));
        codeCoding.setCode(usiIdentifier);
        codeCoding.setDisplay(usiText);
        CodeableConcept codeCodeableConcept = new CodeableConcept();
        codeCodeableConcept.addCoding(codeCoding);
        procedureRequest.setCode(codeCodeableConcept);

        // Add the request date and time.
        Date requestDateTime = obr.getRequestedDateTime().getTimeOfAnEvent().getValueAsDate();
        procedureRequest.setAuthoredOn(requestDateTime);

        // Fill in other mandatory elements of ProcedureRequest.
        procedureRequest.setStatus(ProcedureRequest.ProcedureRequestStatus.COMPLETED);
        procedureRequest.setIntent(ProcedureRequest.ProcedureRequestIntent.FILLERORDER);

        return procedureRequest;
    }

    /**
     * Processes the Observation Request (OBR) part of the HL7 v2 message, and returns a DiagnosticReport resource.
     */
    private DiagnosticReport extractDiagnosticReport(OBR obr) throws DataTypeException {
        DiagnosticReport diagnosticReport = new DiagnosticReport();
        String fillerOrderNumber = obr.getFillerOrderNumber().getEntityIdentifier().toString();

        // Create a logical ID using a hashed version of the filler order number.
        String id = getHashedId(NS_DIAGNOSTIC_REPORT_ID, fillerOrderNumber);
        diagnosticReport.setId(id);

        // Create an identifier using a hashed version of the filler order number.
        Identifier identifier = getHashedIdentifier(NS_DIAGNOSTIC_REPORT_ID, fillerOrderNumber);
        diagnosticReport.getIdentifier().add(identifier);

        // Populate the effective time or time period for the report.
        Date observationDateTime = obr.getObservationDateTime().getTimeOfAnEvent().getValueAsDate();
        Date observationEndDateTime = obr.getObservationEndDateTime().getTimeOfAnEvent().getValueAsDate();
        if (observationDateTime != null && observationEndDateTime != null) {
            Period effectivePeriod = new Period();
            effectivePeriod.setStart(observationDateTime);
            effectivePeriod.setEnd(observationEndDateTime);
            diagnosticReport.setEffective(effectivePeriod);
        } else {
            diagnosticReport.setEffective(new DateTimeType(observationDateTime));
        }

        return diagnosticReport;
    }

    /**
     * Processes an Observation within the HL7 v2 message and returns an Observation resource.
     */
    private Observation extractObservation(OBX obx, String diagnosticReportId) throws DataTypeException {
        Observation observation = new Observation();

        // Create a composite identifier, consisting of the filler order number plus the set ID.
        String compositeId = diagnosticReportId + "-" + obx.getSetIDOBX().getValue();

        // Create a logical ID using a hashed version of the composite identifier.
        String id = getHashedId(NS_OBSERVATION_ID, compositeId);
        observation.setId(id);

        // Create an identifier using a hashed version of the composite identifier.
        Identifier identifier = getHashedIdentifier(NS_OBSERVATION_ID, compositeId);
        observation.getIdentifier().add(identifier);

        CodeableConcept codeableConcept = new CodeableConcept();

        // Populate the code of the Observation.
        String code = obx.getObservationIdentifier().getIdentifier().getValue();
        String system = obx.getObservationIdentifier().getNameOfCodingSystem().getValue();
        String display = obx.getObservationIdentifier().getText().getValue();
        Coding coding = new Coding();
        coding.setCode(code);
        coding.setSystem(getUriForV2CodeSystem(system));
        coding.setDisplay(display);
        codeableConcept.getCoding().add(coding);

        // Add the alternate code as well.
        String alternateCode = obx.getObservationIdentifier().getAlternateIdentifier().getValue();
        String alternateSystem = obx.getObservationIdentifier().getNameOfAlternateCodingSystem().getValue();
        String alternateDisplay = obx.getObservationIdentifier().getAlternateText().getValue();
        Coding alternateCoding = new Coding();
        alternateCoding.setCode(alternateCode);
        alternateCoding.setSystem(getUriForV2CodeSystem(alternateSystem));
        alternateCoding.setDisplay(alternateDisplay);
        codeableConcept.getCoding().add(alternateCoding);

        observation.setCode(codeableConcept);

        // Populate the date/time of the Observation.
        Date date = obx.getDateTimeOfTheObservation().getTimeOfAnEvent().getValueAsDate();
        observation.setEffective(new DateTimeType(date));

        // TODO: Populate Observation Result Status, using a concept map.

        return observation;
    }

    private Identifier getHashedIdentifier(String system, String value) {
        String hashedValue = getHashedId(system, value);
        Identifier identifier = new Identifier();
        identifier.setSystem(system);
        identifier.setValue(hashedValue);
        return identifier;
    }

    private String getHashedId(String system, String value) {
        return DigestUtils.sha256Hex(system + "|" + value);
    }

    /**
     * Selects the best translation from a set of matches out of the result of a translate operation.
     * <p>
     * Currently the way this works is that only `equivalent`, `wider` and `narrower` matches are accepted, and
     * selected using this order of preference.
     */
    private Optional<Parameters.ParametersParameterComponent> selectTranslation(
            List<Parameters.ParametersParameterComponent> matches) {
        // Look for any matches with an `equivalence` of `equivalent`.
        Optional<Parameters.ParametersParameterComponent> equivalentMatch = getMatchWithEquivalence(matches,
                                                                                                    "equivalent");
        if (equivalentMatch.isPresent()) return equivalentMatch;
        // Look for any matches with an `equivalence` of `wider`.
        Optional<Parameters.ParametersParameterComponent> widerMatch = getMatchWithEquivalence(matches, "wider");
        if (widerMatch.isPresent()) return widerMatch;
        // Look for any matches with an `equivalence` of `narrower`.
        // Return the narrower match, or an empty Optional if there are no suitable matches.
        return getMatchWithEquivalence(matches, "narrower");
    }

    /**
     * Takes a list of translate matches, and returns the first one with the matching `equivalence` value.
     */
    private Optional<Parameters.ParametersParameterComponent> getMatchWithEquivalence(
            List<Parameters.ParametersParameterComponent> matches, String equivalenceValue) {
        return matches.stream().filter(match -> {
            Optional<Parameters.ParametersParameterComponent> equivalence =
                    match.getPart().stream().filter(part -> part.getName().equals("equivalence")).findFirst();
            return equivalence.isPresent() && equivalence.get().getValue().toString().equals(
                    equivalenceValue);
        }).findFirst();
    }

}
