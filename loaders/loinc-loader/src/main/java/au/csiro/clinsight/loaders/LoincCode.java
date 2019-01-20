/**
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use
 * is subject to license terms and conditions.
 */

package au.csiro.clinsight.loaders;

import org.hl7.fhir.dstu3.model.Parameters;
import org.hl7.fhir.dstu3.model.ValueSet;

public class LoincCode {

  private ValueSet.ValueSetExpansionContainsComponent expansionComponent;
  private Parameters lookupResponse;

  public LoincCode() {
  }

  public LoincCode(ValueSet.ValueSetExpansionContainsComponent expansionComponent,
      Parameters lookupResponse) {
    this.expansionComponent = expansionComponent;
    this.lookupResponse = lookupResponse;
  }

  public ValueSet.ValueSetExpansionContainsComponent getExpansionComponent() {
    return expansionComponent;
  }

  public void setExpansionComponent(
      ValueSet.ValueSetExpansionContainsComponent expansionComponent) {
    this.expansionComponent = expansionComponent;
  }

  public Parameters getLookupResponse() {
    return lookupResponse;
  }

  public void setLookupResponse(Parameters lookupResponse) {
    this.lookupResponse = lookupResponse;
  }

}
