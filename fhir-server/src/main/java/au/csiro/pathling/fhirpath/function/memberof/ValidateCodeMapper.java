/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.memberof;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.BundleEntryRequestComponent;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.Bundle.HTTPVerb;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.slf4j.MDC;

/**
 * Takes a set of Rows with two columns: (1) a correlation identifier, and; (2) a Coding or
 * CodeableConcept to validate. Returns a set of ValidateCodeResults, which contain the correlation
 * identifier and a boolean result.
 */
@Slf4j
public class ValidateCodeMapper implements MapPartitionsFunction<Row, ValidateCodeResult> {

  private static final long serialVersionUID = 2879761794073649202L;

  @Nonnull
  private final String requestId;

  @Nonnull
  private final TerminologyClientFactory terminologyClientFactory;

  @Nonnull
  private final String valueSetUri;

  @Nonnull
  private final FHIRDefinedType fhirType;

  /**
   * @param requestId An identifier used alongside any logging that the mapper outputs
   * @param terminologyClientFactory Used to create instances of the terminology client on workers
   * @param valueSetUri The identifier of the ValueSet that codes will be validated against
   * @param fhirType The type of the input, either Coding or CodeableConcept
   */
  public ValidateCodeMapper(@Nonnull final String requestId,
      @Nonnull final TerminologyClientFactory terminologyClientFactory,
      @Nonnull final String valueSetUri, @Nonnull final FHIRDefinedType fhirType) {
    this.requestId = requestId;
    this.terminologyClientFactory = terminologyClientFactory;
    this.valueSetUri = valueSetUri;
    this.fhirType = fhirType;
  }

  @Override
  @Nonnull
  public Iterator<ValidateCodeResult> call(@Nullable final Iterator<Row> inputRows) {
    if (inputRows == null || !inputRows.hasNext()) {
      return Collections.emptyIterator();
    }

    // Add the request ID to the logging context, so that we can track the logging for this
    // request across all workers.
    MDC.put("requestId", requestId);

    // Create a terminology client.
    final TerminologyClient terminologyClient = terminologyClientFactory.build(log);

    // Create a Bundle to represent the batch of validate code requests.
    final Bundle validateCodeBatch = new Bundle();
    validateCodeBatch.setType(BundleType.BATCH);

    // Create a list to store the details of the Codings requested - this will be used to
    // correlate requested Codings with responses later on.
    final List<ValidateCodeResult> results = new ArrayList<>();

    // Create a request entry in the batch for each input row.
    inputRows.forEachRemaining(addEntryForInputRow(validateCodeBatch, results));

    // Execute the operation on the terminology server.
    log.info("Sending batch of $validate-code requests to terminology service, " + validateCodeBatch
        .getEntry().size() + " concepts");
    final Bundle validateCodeResult = terminologyClient.batch(validateCodeBatch);

    // Convert each result into a Row.
    for (int i = 0; i < validateCodeResult.getEntry().size(); i++) {
      final BundleEntryComponent entry = validateCodeResult.getEntry().get(i);
      final ValidateCodeResult result = results.get(i);
      if (entry.getResponse().getStatus().startsWith("2")) {
        // If the response was successful, check that it has a `result` parameter with a
        // value of false.
        final Type resultValue = ((Parameters) entry.getResource()).getParameter("result");
        final boolean validated =
            resultValue.hasType("boolean") && ((BooleanType) resultValue).booleanValue();
        result.setResult(validated);
      } else {
        result.setResult(false);
      }
    }

    return results.iterator();
  }

  @Nonnull
  private Consumer<Row> addEntryForInputRow(final Bundle validateCodeBatch,
      final Collection<ValidateCodeResult> results) {
    return inputRow -> {
      // Get the Coding struct out of the Row. If it is null, skip this Row.
      final Row inputCoding = inputRow.getStruct(1);
      if (inputCoding == null) {
        return;
      }
      // Add the hash to an ordered list - we will later update these objects based on index
      // from the ordered results of the $validate-code operation.
      results.add(new ValidateCodeResult(inputRow.getInt(0)));

      // Extract the Coding or CodeableConcept from the Row.
      final Type concept = getCodingOrCodeableConceptFromRow(inputCoding);

      // Construct a Bundle entry containing a validate code request using the Coding or
      // CodeableConcept.
      final BundleEntryComponent entry = createEntryFromCodingOrCodeableConcept(concept);

      // Add the entry to the Bundle.
      validateCodeBatch.addEntry(entry);
    };
  }

  @Nonnull
  private Type getCodingOrCodeableConceptFromRow(final Row inputCoding) {
    final Type concept;
    assert fhirType == FHIRDefinedType.CODING || fhirType == FHIRDefinedType.CODEABLECONCEPT;
    if (fhirType == FHIRDefinedType.CODING) {
      concept = getCodingFromRow(inputCoding);
    } else {
      final CodeableConcept codeableConcept = new CodeableConcept();
      codeableConcept.setId(inputCoding.getString(inputCoding.fieldIndex("id")));
      final List<Row> codingRows = inputCoding.getList(inputCoding.fieldIndex("coding"));
      final List<Coding> codings = codingRows.stream()
          .map(ValidateCodeMapper::getCodingFromRow)
          .collect(Collectors.toList());
      codeableConcept.setCoding(codings);
      codeableConcept.setText(inputCoding.getString(inputCoding.fieldIndex("text")));
      concept = codeableConcept;
    }
    return concept;
  }

  @Nonnull
  private static Coding getCodingFromRow(@Nonnull final Row inputCoding) {
    final Coding coding;
    final String id = inputCoding.getString(inputCoding.fieldIndex("id"));
    final String system = inputCoding.getString(inputCoding.fieldIndex("system"));
    final String version = inputCoding.getString(inputCoding.fieldIndex("version"));
    final String code = inputCoding.getString(inputCoding.fieldIndex("code"));
    final String display = inputCoding.getString(inputCoding.fieldIndex("display"));
    coding = new Coding(system, code, display);
    coding.setId(id);
    coding.setVersion(version);
    // Conditionally set the `userSelected` field based on whether it is null in the data -
    // missingness is significant in FHIR.
    final Boolean userSelected = (Boolean) inputCoding.get(inputCoding.fieldIndex("userSelected"));
    if (userSelected != null) {
      coding.setUserSelected(userSelected);
    }
    return coding;
  }

  @Nonnull
  private BundleEntryComponent createEntryFromCodingOrCodeableConcept(final Type concept) {
    final BundleEntryComponent entry = new BundleEntryComponent();
    final BundleEntryRequestComponent request = new BundleEntryRequestComponent();
    request.setMethod(HTTPVerb.POST);
    request.setUrl("ValueSet/$validate-code");
    entry.setRequest(request);
    final Parameters parameters = new Parameters();
    final ParametersParameterComponent systemParam = new ParametersParameterComponent();
    systemParam.setName("url");
    systemParam.setValue(new UriType(valueSetUri));
    final ParametersParameterComponent conceptParam = new ParametersParameterComponent();
    conceptParam.setName(fhirType == FHIRDefinedType.CODING
                         ? "coding"
                         : "codeableConcept");
    conceptParam.setValue(concept);
    parameters.addParameter(systemParam);
    parameters.addParameter(conceptParam);
    entry.setResource(parameters);
    return entry;
  }

}
