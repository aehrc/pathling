import ca.uhn.fhir.context.FhirContext;
import org.hl7.fhir.r4.model.*;
public class BundleIdProbe {
  static String B(String full, String id) {
    return "<Bundle xmlns=\"http://hl7.org/fhir\"><type value=\"collection\"/><entry>" + (full==null?"":"<fullUrl value=\""+full+"\"/>")
     + "<resource><Observation>" + (id==null?"":"<id value=\""+id+"\"/>") + "<status value=\"final\"/><code><text value=\"c\"/></code></Observation></resource></entry></Bundle>";
  }
  public static void main(String[] a) {
    FhirContext c = FhirContext.forR4();
    String[][] cases = {{"urn:uuid:o1","o1"},{"urn:uuid:zzz","o1"},{"urn:uuid:o1","other"},{"http://ex/Observation/o1","o1"},{"urn:uuid:p1","o1"},{null,"o1"},{"urn:uuid:zzz",null}};
    for (String[] k : cases) {
      Bundle b = (Bundle) c.newXmlParser().parseResource(B(k[0], k[1]));
      Resource r = b.getEntryFirstRep().getResource();
      System.out.printf("fullUrl=%-26s id=%-6s -> idElement=%-34s json=%s%n", k[0], k[1], r.getIdElement().getValue(), c.newJsonParser().encodeResourceToString(r));
    }
  }
}
