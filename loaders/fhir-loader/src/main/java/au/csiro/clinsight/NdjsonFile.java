/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import ca.uhn.fhir.context.FhirContext;
import java.io.InputStream;
import java.util.Iterator;
import javax.annotation.Nonnull;
import org.hl7.fhir.instance.model.api.IBaseResource;

@SuppressWarnings("unused")
public class NdjsonFile implements Iterable<IBaseResource> {

  private final NdjsonIterator ndjsonIterator;

  public NdjsonFile(FhirContext fhirContext, InputStream inputStream) {
    this.ndjsonIterator = new NdjsonIterator(fhirContext, inputStream);
  }

  @Nonnull
  @Override
  public Iterator<IBaseResource> iterator() {
    return ndjsonIterator;
  }

  public int getCurrentIndex() {
    return ndjsonIterator.getCurrentIndex();
  }

}
