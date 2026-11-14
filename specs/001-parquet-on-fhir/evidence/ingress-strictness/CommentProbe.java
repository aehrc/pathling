import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.StrictErrorHandler;
import org.hl7.fhir.r4.model.Patient;

/** Whether HAPI 8.10.0 accepts and keeps XML comments carrying type hints. */
public class CommentProbe {
  public static void main(final String[] args) {
    final String ns = "xmlns=\"http://hl7.org/fhir\"";
    final String[][] cases = {
        {"comment inside a described primitive", "<Patient " + ns + "><birthDate value=\"1980-01-01\"><!--pof:type=date--></birthDate></Patient>"},
        {"comment before a described primitive", "<Patient " + ns + "><!--pof:type=date--><birthDate value=\"1980-01-01\"/></Patient>"},
        {"comment inside an unknown element", "<Patient " + ns + "><foo value=\"1.5\"><!--pof:type=decimal--></foo></Patient>"},
    };
    final FhirContext ctx = FhirContext.forR4();
    for (final String[] c : cases) {
      System.out.println("== " + c[0]);
      for (final boolean strict : new boolean[]{false, true}) {
        try {
          final var parser = ctx.newXmlParser();
          if (strict) parser.setParserErrorHandler(new StrictErrorHandler());
          final Patient p = parser.parseResource(Patient.class, c[1]);
          System.out.printf("  %-8s ok  xml=%s%n           json=%s%n", strict ? "strict" : "lenient",
              ctx.newXmlParser().encodeResourceToString(p), ctx.newJsonParser().encodeResourceToString(p));
        } catch (final Exception e) {
          System.out.printf("  %-8s FAIL %s%n", strict ? "strict" : "lenient", e.getMessage());
        }
      }
    }
  }
}
