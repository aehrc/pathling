import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.parser.IParserErrorHandler;
import ca.uhn.fhir.parser.StrictErrorHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * Parses each fixture with HAPI as Pathling 9.9.0 did (newJsonParser(), the context's default
 * error handler) and with StrictErrorHandler, records every error-handler callback, and re-encodes
 * the result so what was kept, coerced or dropped is visible.
 *
 * <p>Usage: {@code java -cp <runtime-9.9.0.jar> HapiProbe.java fixtures.tsv}
 */
public class HapiProbe {

  public static void main(final String[] args) throws Exception {
    final FhirContext ctx = FhirContext.forR4();
    // FhirContext defaults its parsers to a new LenientErrorHandler(), which 9.9.0 never replaced.
    final IParserErrorHandler lenient = new ca.uhn.fhir.parser.LenientErrorHandler();
    System.out.println("# default handler: " + lenient.getClass().getName());
    for (final String line : Files.readAllLines(Path.of(args[0]), StandardCharsets.UTF_8)) {
      if (line.isBlank()) continue;
      final String[] f = line.split("\t", 3);
      System.out.println("## " + f[0] + " [" + f[1] + "]");
      System.out.println("in:      " + f[2]);
      run(ctx, "default", lenient, f[2]);
      run(ctx, "strict", new StrictErrorHandler(), f[2]);
    }
  }

  static void run(
      final FhirContext ctx, final String label, final IParserErrorHandler delegate, final String json) {
    final List<String> calls = new ArrayList<>();
    final IParserErrorHandler recording =
        (IParserErrorHandler)
            Proxy.newProxyInstance(
                IParserErrorHandler.class.getClassLoader(),
                new Class<?>[] {IParserErrorHandler.class},
                (proxy, method, a) -> {
                  calls.add(method.getName() + describe(a));
                  try {
                    return method.invoke(delegate, a);
                  } catch (InvocationTargetException e) {
                    throw e.getCause();
                  }
                });
    final IParser parser = ctx.newJsonParser().setParserErrorHandler(recording);
    parser.setOverrideResourceIdWithBundleEntryFullUrl(false);
    String out;
    try {
      final IBaseResource r = parser.parseResource(json);
      out = "OK   " + ctx.newJsonParser().encodeResourceToString(r);
    } catch (Exception e) {
      out = "FAIL " + e.getClass().getSimpleName() + ": " + e.getMessage();
    }
    System.out.println(String.format("%-8s %s", label + ":", out));
    calls.forEach(c -> System.out.println("         ~ " + c));
  }

  static String describe(final Object[] a) {
    if (a == null) return "()";
    return Arrays.stream(a)
        .map(o -> o == null ? "null" : o.getClass().getSimpleName().contains("Location")
            ? "loc=" + o : String.valueOf(o))
        .toList()
        .toString();
  }
}
