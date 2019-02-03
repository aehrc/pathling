/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import static au.csiro.clinsight.utilities.Preconditions.checkNotNull;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.CodeType;
import org.hl7.fhir.dstu3.model.ConceptMap;
import org.hl7.fhir.dstu3.model.ElementDefinition;
import org.hl7.fhir.dstu3.model.IntegerType;
import org.hl7.fhir.dstu3.model.Parameters;
import org.hl7.fhir.dstu3.model.Property;
import org.hl7.fhir.dstu3.model.Resource;
import org.hl7.fhir.dstu3.model.StringType;
import org.hl7.fhir.dstu3.model.StructureDefinition;
import org.hl7.fhir.dstu3.model.UriType;
import org.hl7.fhir.dstu3.model.ValueSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author John Grimes
 */
public class OldTerminologyClient {

  // TODO: Convert this into an interface (annotation-driven client), extending IRestfulClient.

  private static final Logger logger = LoggerFactory.getLogger(OldTerminologyClient.class);
  private TerminologyClientConfiguration configuration;
  private IGenericClient client;
  private Map<String, Bundle> searchStructureDefinitionsByUrlCache;

  public OldTerminologyClient(TerminologyClientConfiguration configuration,
      FhirContext fhirContext) {
    checkNotNull(configuration.getTerminologyServerUrl(), "Must supply terminology server URL");
    checkNotNull(configuration.getExpansionCount(), "Must supply expansion count");

    logger.info("Creating new OldTerminologyClient: " + configuration);
    this.configuration = configuration;
    this.client = fhirContext.newRestfulGenericClient(configuration.getTerminologyServerUrl());
    searchStructureDefinitionsByUrlCache = new ConcurrentHashMap<>();
  }

  /**
   * Performs a type-level ValueSet$expand operation, returning the entire expansion as a set of
   * ExpansionResult objects.
   *
   * @see <a href="http://hl7.org/fhir/STU3/valueset-operations.html#expand">ValueSet$expand</a> in
   * the FHIR STU3 specification.
   */
  public List<ExpansionResult> expand(String url) {
    checkNotNull(url, "Must supply url");

    long start = System.nanoTime();

    List<ExpansionResult> results = new ArrayList<>();
    int offset = 0;
    int numberOfRequests = 0;

    while (true) {
      Parameters inParams = new Parameters();
      inParams.addParameter().setName("url").setValue(new UriType(url));
      inParams.addParameter().setName("count")
          .setValue(new IntegerType(configuration.getExpansionCount()));
      if (offset != 0) {
        inParams.addParameter().setName("offset").setValue(new IntegerType(offset));
      }
      Parameters outParams = client.operation()
          .onType(ValueSet.class)
          .named("expand")
          .withParameters(inParams)
          .execute();
      numberOfRequests += 1;
      ValueSet result = (ValueSet) outParams.getParameter().get(0).getResource();
      results.addAll(getExpansionResults(result));
      if (result.getExpansion().getTotal() > results.size()) {
        offset = results.size();
      } else {
        break;
      }
    }

    double elapsedMs = TimeUnit.MILLISECONDS
        .convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
    logger.info("Expanded " + results.size() + " codes from " + url + " ValueSet in " + String
        .format("%.1f", elapsedMs) + " ms (" + numberOfRequests + " requests)");

    return results;
  }

  /**
   * Performs a type-level ValueSet$expand operation, using the supplied `url`, `count` and
   * `offset`.
   *
   * @see <a href="http://hl7.org/fhir/STU3/valueset-operations.html#expand">ValueSet$expand</a> in
   * the FHIR STU3 specification.
   */
  public ValueSet expand(String url, String count, int offset) {
    checkNotNull(url, "Must supply url");
    checkNotNull(url, "Must supply count");
    checkNotNull(url, "Must supply offset");

    long start = System.nanoTime();

    Parameters inParams = new Parameters();
    inParams.addParameter().setName("url").setValue(new UriType(url));
    inParams.addParameter().setName("count").setValue(new StringType(count));
    inParams.addParameter().setName("offset").setValue(new IntegerType(offset));
    Parameters outParams = client.operation()
        .onType(ValueSet.class)
        .named("expand")
        .withParameters(inParams)
        .execute();
    ValueSet result = (ValueSet) outParams.getParameter().get(0).getResource();

    double elapsedMs = TimeUnit.MILLISECONDS
        .convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
    logger.info("Expanded " + result.getExpansion().getContains().size() + " codes from " + url
        + " ValueSet in " + String.format("%.1f", elapsedMs) + " ms");

    return result;
  }

  /**
   * Performs a type-level CodeSystem$translate operation.
   *
   * @see <a href="http://hl7.org/fhir/STU3/conceptmap-operations.html#translate">ConceptMap$translate</a>
   * in the FHIR STU3 specification.
   */
  public Parameters translate(String code, String system, String source, String target) {
    checkNotNull(code, "Must supply code");
    checkNotNull(system, "Must supply system");
    checkNotNull(source, "Must supply source");
    checkNotNull(target, "Must supply target");

    long start = System.nanoTime();

    Parameters inParams = new Parameters();
    inParams.addParameter().setName("code").setValue(new CodeType(code));
    inParams.addParameter().setName("system").setValue(new UriType(system));
    inParams.addParameter().setName("source").setValue(new UriType(source));
    inParams.addParameter().setName("target").setValue(new UriType(target));
    Parameters outParams = client.operation()
        .onType(ConceptMap.class)
        .named("translate")
        .withParameters(inParams)
        .execute();

    double elapsedMs = TimeUnit.MILLISECONDS
        .convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
    logger.info(
        "Translated " + system + "|" + code + " (" + source + " > " + target + ") in " + String
            .format("%.1f", elapsedMs) + " ms");

    return outParams;
  }

  /**
   * Performs a batch of CodeSystem$lookup operations, using the supplied list of codes and
   * returning the requested properties for each code.
   *
   * @see <a href="http://hl7.org/fhir/STU3/codesystem-operations.html#lookup">CodeSystem$lookup</a>
   * in the FHIR STU3 specification.
   */
  public Bundle batchLookup(String system, List<String> codes, List<String> properties) {
    checkNotNull(system, "Must supply system");
    checkNotNull(codes, "Must supply codes");
    checkNotNull(properties, "Must supply properties");

    long start = System.nanoTime();

    Bundle bundle = new Bundle();
    bundle.setType(Bundle.BundleType.BATCH);
    for (String code : codes) {
      Parameters inParams = new Parameters();
      inParams.addParameter().setName("system").setValue(new UriType(system));
      inParams.addParameter().setName("code").setValue(new CodeType(code));
      for (String property : properties) {
        inParams.addParameter().setName("property").setValue(new CodeType(property));
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
        "Executed batch lookup of " + codes.size() + " codes from " + system +
            " CodeSystem in " + String.format("%.1f", elapsedMs) + " ms");

    return result;
  }

  public Bundle searchStructureDefinitions() {
    return client.search()
        .forResource(StructureDefinition.class)
        .returnBundle(Bundle.class)
        .execute();
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

  /**
   * Get a list of ExpansionResult objects that contain the information from an expansion result
   * ValueSet.
   */
  private static List<ExpansionResult> getExpansionResults(ValueSet valueSet) {
    return valueSet.getExpansion().getContains().stream().map(
        contains -> {
          ExpansionResult result = new ExpansionResult();
          result.setSystem(contains.getSystem());
          result.setVersion(contains.getVersion());
          result.setCode(contains.getCode());
          result.setDisplay(contains.getDisplay());
          return result;
        }).collect(Collectors.toList());
  }

  public IGenericClient getClient() {
    return client;
  }
}
