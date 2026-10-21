import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.StrictErrorHandler;
import org.hl7.fhir.r4.model.Patient;

/** Whether HAPI 8.10.0 keeps XML comments that carry undescribed content, by where they sit. */
public class CommentCarrierProbe {
  public static void main(final String[] args) {
    final String ns = "xmlns=\"http://hl7.org/fhir\"";
    final String c = "<!--pof:extra <foo value=\"1.5\" pof:type=\"decimal\"/>-->";
    final String[][] cases = {
        {"before a root child", "<Patient " + ns + ">" + c + "<birthDate value=\"1980-01-01\"/></Patient>"},
        {"after the last root child", "<Patient " + ns + "><birthDate value=\"1980-01-01\"/>" + c + "</Patient>"},
        {"only content of the root", "<Patient " + ns + ">" + c + "</Patient>"},
        {"before a nested child", "<Patient " + ns + "><name>" + c + "<family value=\"Smith\"/></name></Patient>"},
        {"after the last nested child", "<Patient " + ns + "><name><family value=\"Smith\"/>" + c + "</name></Patient>"},
        {"inside a primitive", "<Patient " + ns + "><birthDate value=\"1980-01-01\">" + c + "</birthDate></Patient>"},
        {"before a repeating item", "<Patient " + ns + "><name><given value=\"A\"/>" + c + "<given value=\"B\"/></name></Patient>"},
    };
    final FhirContext ctx = FhirContext.forR4();
    for (final String[] k : cases) {
      System.out.println("== " + k[0]);
      for (final boolean strict : new boolean[]{false, true}) {
        try {
          final var parser = ctx.newXmlParser();
          if (strict) parser.setParserErrorHandler(new StrictErrorHandler());
          final Patient p = parser.parseResource(Patient.class, k[1]);
          final String out = ctx.newXmlParser().encodeResourceToString(p);
          System.out.printf("  %-8s ok  kept=%s  xml=%s%n", strict ? "strict" : "lenient",
              out.contains("pof:extra"), out);
        } catch (final Exception e) {
          System.out.printf("  %-8s FAIL %s%n", strict ? "strict" : "lenient", e.getMessage());
        }
      }
    }
  }
}
