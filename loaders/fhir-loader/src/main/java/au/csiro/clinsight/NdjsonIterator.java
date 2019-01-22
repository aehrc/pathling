/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NdjsonIterator implements Iterator<IBaseResource> {

  private static final Logger logger = LoggerFactory.getLogger(NdjsonIterator.class);
  private IParser parser;
  private BufferedReader reader;
  private int currentIndex;

  public NdjsonIterator(FhirContext fhirContext, InputStream inputStream) {
    parser = fhirContext.newJsonParser();
    reader = new BufferedReader(new InputStreamReader(inputStream));
    currentIndex = 0;
  }

  @Override
  public boolean hasNext() {
    try {
      return reader.ready();
    } catch (IOException e) {
      logger.error("Error occurred while checking ready status", e);
    }
    return false;
  }

  @Override
  public IBaseResource next() {
    String json = null;
    try {
      json = reader.readLine();
      currentIndex++;
    } catch (IOException e) {
      logger.error("Error occurred while trying to read line", e);
    }
    return parser.parseResource(json);
  }

  public int getCurrentIndex() {
    return currentIndex;
  }

}
