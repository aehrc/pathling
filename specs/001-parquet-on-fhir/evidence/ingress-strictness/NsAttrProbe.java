import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParserErrorHandler;
import ca.uhn.fhir.parser.LenientErrorHandler;
import ca.uhn.fhir.parser.StrictErrorHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import org.hl7.fhir.r4.model.Patient;

/** How HAPI 8.10.0 reads FHIR XML carrying attributes and elements from other namespaces. */
public class NsAttrProbe {

  static final String NS = "xmlns=\"http://hl7.org/fhir\" xmlns:pof=\"urn:example:pof\" "
      + "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"";

  public static void main(final String[] args) {
    final String[][] cases = {
        {"baseline", "<Patient " + NS + "><birthDate value=\"1980-01-01\"/></Patient>"},
        {"pof attr on a described primitive",
            "<Patient " + NS + "><birthDate value=\"1980-01-01\" pof:type=\"date\"/></Patient>"},
        {"pof attr on the root", "<Patient " + NS + " pof:type=\"Patient\"><birthDate value=\"1980-01-01\"/></Patient>"},
        {"pof attr on a complex element",
            "<Patient " + NS + "><name pof:type=\"HumanName\"><family value=\"Smith\"/></name></Patient>"},
        {"unknown element, no attr", "<Patient " + NS + "><foo value=\"1.5\"/></Patient>"},
        {"unknown element with pof attr", "<Patient " + NS + "><foo value=\"1.5\" pof:type=\"decimal\"/></Patient>"},
        {"xsi:type on a described primitive",
            "<Patient " + NS + "><birthDate value=\"1980-01-01\" xsi:type=\"date\"/></Patient>"},
        {"element in the pof namespace", "<Patient " + NS + "><pof:foo value=\"1.5\"/><birthDate value=\"1980-01-01\"/></Patient>"},
        {"unprefixed unknown attr on a described primitive",
            "<Patient " + NS + "><birthDate value=\"1980-01-01\" type=\"date\"/></Patient>"},
    };
    final FhirContext ctx = FhirContext.forR4();
    for (final String[] c : cases) {
      System.out.println("== " + c[0]);
      run(ctx, "lenient", new LenientErrorHandler(), c[1]);
      run(ctx, "strict", new StrictErrorHandler(), c[1]);
    }
  }

  static void run(final FhirContext ctx, final String label, final IParserErrorHandler real,
      final String xml) {
    final List<String> calls = new ArrayList<>();
    // Records each callback, then delegates, so the real handler still decides whether to throw.
    final IParserErrorHandler recording = (IParserErrorHandler) Proxy.newProxyInstance(
        IParserErrorHandler.class.getClassLoader(), new Class<?>[]{IParserErrorHandler.class},
        (p, m, a) -> {
          calls.add(m.getName() + (a != null && a.length > 1 && a[1] != null ? "(" + a[1] + ")" : ""));
          try {
            return m.invoke(real, a);
          } catch (final java.lang.reflect.InvocationTargetException e) {
            throw e.getCause();
          }
        });
    try {
      final Patient p = ctx.newXmlParser().setParserErrorHandler(recording)
          .parseResource(Patient.class, xml);
      final String out = ctx.newXmlParser().encodeResourceToString(p);
      System.out.printf("  %-8s ok   callbacks=%s%n           out=%s%n", label, calls, out);
    } catch (final Exception e) {
      System.out.printf("  %-8s FAIL callbacks=%s%n           %s%n", label, calls,
          e.getMessage().replaceAll("\\s+", " "));
    }
  }
}
