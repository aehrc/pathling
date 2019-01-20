/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.CodeType;
import org.hl7.fhir.dstu3.model.ConceptMap;
import org.hl7.fhir.dstu3.model.ElementDefinition;
import org.hl7.fhir.dstu3.model.IntegerType;
import org.hl7.fhir.dstu3.model.Parameters;
import org.hl7.fhir.dstu3.model.Property;
import org.hl7.fhir.dstu3.model.Resource;
import org.hl7.fhir.dstu3.model.StructureDefinition;
import org.hl7.fhir.dstu3.model.UriType;
import org.hl7.fhir.dstu3.model.ValueSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Add documentation.

/**
 * @author John Grimes
 */
public class TerminologyClient {

  private static final Logger logger = LoggerFactory.getLogger(TerminologyClient.class);
  private FhirContext fhirContext;
  private String serverBase;
  private IGenericClient client;
  private Map<String, Bundle> searchStructureDefinitionsByUrlCache;

  public TerminologyClient(FhirContext fhirContext, String serverBase) {
    this.fhirContext = fhirContext;
    this.serverBase = serverBase;
    this.client = fhirContext.newRestfulGenericClient(serverBase);
    searchStructureDefinitionsByUrlCache = new ConcurrentHashMap<>();
  }

  // TODO: Add documentation.
  public ValueSet expand(UriType url, IntegerType count, IntegerType offset) {
    long start = System.nanoTime();
    Parameters inParams = new Parameters();
    inParams.addParameter().setName("url").setValue(url);
    inParams.addParameter().setName("count").setValue(count);
    inParams.addParameter().setName("offset").setValue(offset);
    Parameters outParams = client.operation()
        .onType(ValueSet.class)
        .named("expand")
        .withParameters(inParams)
        .execute();
    ValueSet result = (ValueSet) outParams.getParameter().get(0).getResource();
    double elapsedMs = TimeUnit.MILLISECONDS
        .convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
    logger.info("Expanded " + result.getExpansion().getContains().size() + " codes from " + url
        .asStringValue() +
        " ValueSet in " + String.format("%.1f", elapsedMs) + " ms");
    return result;
  }

  /**
   * Performs a type-level CodeSystem$translate operation against the configured terminology
   * server.
   *
   * @param code The code that is to be translated
   * @param system The system for the code that is to be translated
   * @param source Identifies the value set used when the concept (system/code pair) was chosen
   * @param target Identifies the value set in which a translation is sought
   * @throws IllegalArgumentException All arguments must be non-null.
   * @see <a href="http://hl7.org/fhir/STU3/conceptmap-operations.html#translate">ConceptMap$translate</a>
   * in the FHIR STU3 specification.
   */
  public Parameters translate(CodeType code, UriType system, UriType source, UriType target) {
    if (code == null || system == null || source == null || target == null) {
      throw new IllegalArgumentException(
          "Must provide non-null code, system, source and target arguments.");
    }
    Parameters inParams = new Parameters();
    inParams.addParameter().setName("code").setValue(code);
    inParams.addParameter().setName("system").setValue(system);
    inParams.addParameter().setName("source").setValue(source);
    inParams.addParameter().setName("target").setValue(target);
    return client.operation()
        .onType(ConceptMap.class)
        .named("translate")
        .withParameters(inParams)
        .execute();
  }

  // TODO: Add documentation.
  public Bundle batchLookup(UriType system, List<CodeType> codes, List<CodeType> properties) {
    long start = System.nanoTime();
    Bundle bundle = new Bundle();
    bundle.setType(Bundle.BundleType.BATCH);
    for (CodeType code : codes) {
      Parameters inParams = new Parameters();
      inParams.addParameter().setName("system").setValue(system);
      inParams.addParameter().setName("code").setValue(code);
      for (CodeType property : properties) {
        inParams.addParameter().setName("property").setValue(property);
      }
      bundle.addEntry()
          .setResource(inParams)
          .getRequest()
          .setMethod(Bundle.HTTPVerb.POST)
          .setUrl("CodeSystem/$lookup");
    }
    Bundle result = client
        .transaction()
        .withBundle(bundle)
        .execute();
    double elapsedMs = TimeUnit.MILLISECONDS
        .convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
    logger.info(
        "Executed batch lookup of " + codes.size() + " codes from " + system.asStringValue() +
            " CodeSystem in " + String.format("%.1f", elapsedMs) + " ms");
    return result;
  }

  // TODO: Add documentation.
  public Bundle searchStructureDefinitionsByUrl(String url) {
    return searchStructureDefinitionsByUrlCache.computeIfAbsent(url, u -> {
      long start = System.nanoTime();
      Bundle result = client.search()
          .forResource(StructureDefinition.class)
          .where(StructureDefinition.URL.matches().value(u))
          .returnBundle(Bundle.class)
          .execute();
      double elapsedMs = TimeUnit.MILLISECONDS
          .convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
      logger.info(
          "Searched for StructureDefinitions matching " + u + " in " + String
              .format("%.1f", elapsedMs) +
              " ms");
      return result;
    });
  }

  // TODO: Add documentation.
  public ElementDefinition getElementDefinitionForProperty(Resource resource, Property property) {
    String structureDefinitionUrl =
        "http://hl7.org/fhir/StructureDefinition/" + resource.fhirType();
    Bundle results = searchStructureDefinitionsByUrl(structureDefinitionUrl);
    Optional<StructureDefinition> maybeStructureDefinition =
        results.getEntry().stream().map(entry -> (StructureDefinition) entry.getResource())
            .findFirst();
    if (maybeStructureDefinition.isPresent()) {
      StructureDefinition structureDefinition = maybeStructureDefinition.get();
      // TODO: Support matching of elements that are not at the top level of the resource.
      Optional<ElementDefinition> maybeElementDefinition =
          structureDefinition.getSnapshot()
              .getElement()
              .stream()
              .filter(elementDefinition -> elementDefinition
                  .getPath()
                  .equals(resource.fhirType() + "." + property.getName()))
              .findFirst();
      return maybeElementDefinition.orElse(null);
    } else {
      return null;
    }
  }

}
