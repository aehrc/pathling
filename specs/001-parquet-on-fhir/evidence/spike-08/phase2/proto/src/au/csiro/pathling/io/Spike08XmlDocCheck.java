package au.csiro.pathling.io;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import org.apache.spark.sql.api.java.UDF1;
import org.codehaus.stax2.XMLStreamReader2;

/**
 * Spike 08 prototype (throwaway), the document check the from_xml route needs and cannot make itself:
 * the text is one well-formed XML document whose root is in the FHIR namespace, with nothing after the
 * root. from_xml reads the root and ignores what follows it. Returns null where the document passes,
 * else the reason. A full StAX pass per document; its cost is part of the route's.
 */
public final class Spike08XmlDocCheck implements UDF1<String, String> {

  private static final long serialVersionUID = 1L;

  @Override
  public String call(String doc) {
    if (doc == null) {
      return null;
    }
    try {
      final XMLStreamReader2 r = Spike08Xml.reader(doc);
      while (r.hasNext() && r.next() != XMLStreamConstants.START_ELEMENT) {
        // Up to the root.
      }
      if (!Spike08Xml.FHIR.equals(r.getNamespaceURI())) {
        return "namespace";
      }
      while (r.hasNext()) {
        r.next();
      }
      return null;
    } catch (XMLStreamException e) {
      return "malformed";
    }
  }
}
