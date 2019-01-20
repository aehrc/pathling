/**
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use
 * is subject to license terms and conditions.
 */

package au.csiro.clinsight;

import ca.uhn.fhir.context.FhirContext;
import java.io.InputStream;
import java.util.Iterator;
import org.hl7.fhir.instance.model.api.IBaseResource;

public class NdjsonFile implements Iterable<IBaseResource> {

  private NdjsonIterator ndjsonIterator;

  public NdjsonFile(FhirContext fhirContext, InputStream inputStream) {
    this.ndjsonIterator = new NdjsonIterator(fhirContext, inputStream);
  }

  @Override
  public Iterator<IBaseResource> iterator() {
    return ndjsonIterator;
  }

  public int getCurrentIndex() {
    return ndjsonIterator.getCurrentIndex();
  }

}
