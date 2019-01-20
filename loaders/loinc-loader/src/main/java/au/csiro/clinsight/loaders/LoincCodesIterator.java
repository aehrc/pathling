/**
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use
 * is subject to license terms and conditions.
 */

package au.csiro.clinsight.loaders;

import au.csiro.clinsight.TerminologyClient;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.CodeType;
import org.hl7.fhir.dstu3.model.IntegerType;
import org.hl7.fhir.dstu3.model.Parameters;
import org.hl7.fhir.dstu3.model.ResourceType;
import org.hl7.fhir.dstu3.model.UriType;
import org.hl7.fhir.dstu3.model.ValueSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoincCodesIterator implements Iterator<LoincCode> {

  private static final Logger logger = LoggerFactory.getLogger(LoincCodesIterator.class);
  private static final List<CodeType> lookupProperties = Arrays.asList(
      "designation",
      "STATUS",
      "COMPONENT",
      "METHOD_TYP",
      "PROPERTY",
      "SCALE_TYP",
      "SYSTEM",
      "TIME_ASPCT",
      "CLASSTYPE",
      "answer-list",
      "parent",
      "child",
      "CLASS",
      "EXAMPLE_UCUM_UNITS",
      "ORDER_OBS",
      "CONSUMER_NAME"
  ).stream().map(code -> new CodeType(code)).collect(Collectors.toList());
  private static String loincCodeSystemUri = "http://loinc.org";
  private static String loincValueSetUri = "http://loinc.org/vs";
  private int pageSize;
  private TerminologyClient terminologyClient;
  private List<LoincCode> remaining = new ArrayList<>();
  private int total;
  private int currentOffset = 0;
  private int currentIndex = 0;

  public LoincCodesIterator(TerminologyClient terminologyClient, int pageSize) {
    this.terminologyClient = terminologyClient;
    this.pageSize = pageSize;
    retrievePage();
  }

  private void retrievePage() {
    ValueSet expansionResult = terminologyClient.expand(new UriType(loincValueSetUri),
        new IntegerType(pageSize),
        new IntegerType(currentOffset));
    // Get a page of expansion results.
    List<CodeType> codes = expansionResult
        .getExpansion()
        .getContains()
        .stream()
        .map(expansionComponent -> {
          return new CodeType(expansionComponent.getCode());
        }).collect(Collectors.toList());
    List<ValueSet.ValueSetExpansionContainsComponent> expansionResults =
        expansionResult.getExpansion().getContains();

    // Lookup all the codes in the expansion.
    Bundle lookupResult = terminologyClient
        .batchLookup(new UriType(loincCodeSystemUri), codes, lookupProperties);
    List<Parameters> lookupResults =
        lookupResult.getEntry()
            .stream()
            .map(entry -> entry.getResource() != null &&
                entry.getResource().getResourceType() == ResourceType.Parameters
                ? (Parameters) entry.getResource()
                : null)
            .collect(Collectors.toList());

    // Push the results of the expansion onto the end of the `remaining` list.
    if (expansionResults.size() != lookupResults.size()) {
      logger.warn("Lookup result count does not match expansion size");
    }
    for (int i = 0; i < expansionResults.size(); i++) {
      LoincCode loincCode = new LoincCode();
      loincCode.setExpansionComponent(expansionResults.get(i));
      loincCode.setLookupResponse(lookupResults.get(i));
      remaining.add(loincCode);
    }

    total = expansionResult.getExpansion().getTotal();
    currentOffset = Math.min(currentOffset + pageSize, total);
  }

  @Override
  public boolean hasNext() {
    return !remaining.isEmpty();
  }

  @Override
  public LoincCode next() {
    LoincCode code = remaining.remove(0);
    currentIndex++;
    if (!hasNext() && !(currentOffset == total)) {
      retrievePage();
    }
    return code;
  }

  public int getTotal() {
    return total;
  }

  public int getCurrentIndex() {
    return currentIndex;
  }

}
