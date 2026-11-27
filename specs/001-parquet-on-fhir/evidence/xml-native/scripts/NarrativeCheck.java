import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;
import java.util.Objects;
import org.hl7.fhir.r4.model.Base;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.DomainResource;
import org.hl7.fhir.utilities.xhtml.XhtmlNode;
import org.hl7.fhir.utilities.xhtml.XhtmlParser;

/**
 * Scratch prototype: a StAX pass that rewrites each resource's own narrative {@code div} as
 * {@code <div value="..."/>} before Spark reads it, and the inverse pass for output.
 *
 * <p>Usage: java -cp library-runtime.jar NarrativeCheck.java edges | corpus DIR | dump FILE
 */
public class NarrativeCheck {

  static final String XHTML = "http://www.w3.org/1999/xhtml";
  static final XMLInputFactory IN = XMLInputFactory.newFactory();
  static final XMLOutputFactory OUT = XMLOutputFactory.newFactory();

  static {
    IN.setProperty(XMLInputFactory.SUPPORT_DTD, false);
    IN.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
    IN.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, true);
  }

  /** The rewritten document, the narratives taken out of it and what was noticed on the way. */
  static final class Rewritten {
    String xml;
    final List<String> narratives = new ArrayList<>();
    final List<String> findings = new ArrayList<>();
  }

  // ---------------------------------------------------------------------------------------------
  // Shared event copying.
  // ---------------------------------------------------------------------------------------------

  static void start(XMLStreamReader r, XMLStreamWriter w, List<Map<String, String>> scope)
      throws XMLStreamException {
    final String prefix = r.getPrefix() == null ? "" : r.getPrefix();
    final String ns = r.getNamespaceURI() == null ? "" : r.getNamespaceURI();
    if (ns.isEmpty()) {
      w.writeStartElement(r.getLocalName());
    } else {
      w.writeStartElement(prefix, r.getLocalName(), ns);
    }
    final Map<String, String> declared = new HashMap<>();
    for (int i = 0; i < r.getNamespaceCount(); i++) {
      final String p = r.getNamespacePrefix(i) == null ? "" : r.getNamespacePrefix(i);
      declared.put(p, r.getNamespaceURI(i));
    }
    // A subtree copied out of a larger document must declare every namespace it uses itself.
    if (scope != null) {
      if (!ns.isEmpty() && !ns.equals(lookup(scope, declared, prefix))) {
        declared.put(prefix, ns);
      }
      for (int i = 0; i < r.getAttributeCount(); i++) {
        final String ap = r.getAttributePrefix(i);
        final String ans = r.getAttributeNamespace(i);
        if (ans != null && !ans.isEmpty() && !ans.equals(lookup(scope, declared, ap))) {
          declared.put(ap, ans);
        }
      }
      scope.add(declared);
    }
    declared.forEach(
        (p, u) -> {
          try {
            if (p.isEmpty()) {
              w.writeDefaultNamespace(u);
            } else {
              w.writeNamespace(p, u);
            }
          } catch (XMLStreamException e) {
            throw new IllegalStateException(e);
          }
        });
    for (int i = 0; i < r.getAttributeCount(); i++) {
      final String ans = r.getAttributeNamespace(i);
      if (ans == null || ans.isEmpty()) {
        w.writeAttribute(r.getAttributeLocalName(i), r.getAttributeValue(i));
      } else {
        w.writeAttribute(
            r.getAttributePrefix(i), ans, r.getAttributeLocalName(i), r.getAttributeValue(i));
      }
    }
  }

  static String lookup(List<Map<String, String>> scope, Map<String, String> own, String prefix) {
    if (own.containsKey(prefix)) {
      return own.get(prefix);
    }
    for (int i = scope.size() - 1; i >= 0; i--) {
      if (scope.get(i).containsKey(prefix)) {
        return scope.get(i).get(prefix);
      }
    }
    return null;
  }

  /** Copies the element the reader is on, and everything inside it, leaving it on the end tag. */
  static void copySubtree(XMLStreamReader r, XMLStreamWriter w) throws XMLStreamException {
    final List<Map<String, String>> scope = new ArrayList<>();
    int depth = 0;
    do {
      switch (r.getEventType()) {
        case XMLStreamConstants.START_ELEMENT -> {
          depth++;
          start(r, w, scope);
        }
        case XMLStreamConstants.END_ELEMENT -> {
          depth--;
          scope.remove(scope.size() - 1);
          w.writeEndElement();
        }
        case XMLStreamConstants.CDATA -> w.writeCData(r.getText());
        case XMLStreamConstants.CHARACTERS, XMLStreamConstants.SPACE ->
            w.writeCharacters(r.getText());
        case XMLStreamConstants.COMMENT -> w.writeComment(r.getText());
        case XMLStreamConstants.PROCESSING_INSTRUCTION ->
            w.writeProcessingInstruction(r.getPITarget(), r.getPIData());
        default -> {}
      }
      if (depth > 0) {
        r.next();
      }
    } while (depth > 0);
  }

  static String escapeAttribute(String s) {
    final StringBuilder b = new StringBuilder();
    for (char c : s.toCharArray()) {
      switch (c) {
        case '&' -> b.append("&amp;");
        case '<' -> b.append("&lt;");
        case '>' -> b.append("&gt;");
        case '"' -> b.append("&quot;");
        // XML normalises these to a space inside an attribute value, so they must be references.
        case '\n' -> b.append("&#10;");
        case '\r' -> b.append("&#13;");
        case '\t' -> b.append("&#9;");
        default -> b.append(c);
      }
    }
    return b.toString();
  }

  // ---------------------------------------------------------------------------------------------
  // Ingress: narrative to attribute.
  // ---------------------------------------------------------------------------------------------

  static Rewritten rewrite(String xml) throws XMLStreamException {
    final Rewritten result = new Rewritten();
    final XMLStreamReader r = IN.createXMLStreamReader(new StringReader(xml));
    final StringWriter sw = new StringWriter();
    final XMLStreamWriter w = OUT.createXMLStreamWriter(sw);
    // One entry per open element: its local name, prefix and namespace, and whether it is contained.
    final Deque<String[]> stack = new ArrayDeque<>();
    // A placeholder that cannot occur in the document, so that substituting it back is unambiguous.
    String nonce = "@@";
    while (xml.contains(nonce + "NARRATIVE")) {
      nonce += "@";
    }
    int contained = 0;
    while (r.hasNext()) {
      final int ev = r.next();
      switch (ev) {
        case XMLStreamConstants.START_ELEMENT -> {
          final String ln = r.getLocalName();
          final String ns = r.getNamespaceURI() == null ? "" : r.getNamespaceURI();
          final String[] parent = stack.peek();
          if (XHTML.equals(ns) && ln.equals("div") && parent != null && parent[0].equals("text")) {
            if (contained > 0) {
              result.findings.add("narrative of a contained resource left as it was");
              stack.push(new String[] {ln, "", ns, "n"});
              start(r, w, null);
            } else {
              final StringWriter cap = new StringWriter();
              final XMLStreamWriter cw = OUT.createXMLStreamWriter(cap);
              copySubtree(r, cw);
              cw.flush();
              final String token = nonce + "NARRATIVE" + result.narratives.size() + nonce;
              result.narratives.add(cap.toString());
              if (parent[2].isEmpty()) {
                w.writeEmptyElement("div");
              } else {
                w.writeEmptyElement(parent[1], "div", parent[2]);
              }
              w.writeAttribute("value", token);
            }
          } else {
            final boolean isContained = ln.equals("contained");
            if (isContained) {
              contained++;
            }
            stack.push(
                new String[] {
                  ln, r.getPrefix() == null ? "" : r.getPrefix(), ns, isContained ? "c" : ""
                });
            start(r, w, null);
          }
        }
        case XMLStreamConstants.END_ELEMENT -> {
          final String[] top = stack.pop();
          if (top[3].equals("c")) {
            contained--;
          }
          w.writeEndElement();
        }
        case XMLStreamConstants.CDATA -> w.writeCData(r.getText());
        case XMLStreamConstants.CHARACTERS, XMLStreamConstants.SPACE ->
            w.writeCharacters(r.getText());
        case XMLStreamConstants.COMMENT -> w.writeComment(r.getText());
        case XMLStreamConstants.PROCESSING_INSTRUCTION ->
            w.writeProcessingInstruction(r.getPITarget(), r.getPIData());
        default -> {}
      }
    }
    w.flush();
    String out = sw.toString();
    for (int i = 0; i < result.narratives.size(); i++) {
      out =
          out.replace("\"" + nonce + "NARRATIVE" + i + nonce + "\"", "\"" + escapeAttribute(result.narratives.get(i)) + "\"");
    }
    result.xml = out;
    return result;
  }

  // ---------------------------------------------------------------------------------------------
  // Egress: attribute to narrative.
  // ---------------------------------------------------------------------------------------------

  static String restore(String xml) throws XMLStreamException {
    final XMLStreamReader r = IN.createXMLStreamReader(new StringReader(xml));
    final StringWriter sw = new StringWriter();
    final XMLStreamWriter w = OUT.createXMLStreamWriter(sw);
    final Deque<String> stack = new ArrayDeque<>();
    while (r.hasNext()) {
      final int ev = r.next();
      switch (ev) {
        case XMLStreamConstants.START_ELEMENT -> {
          final String ln = r.getLocalName();
          final String ns = r.getNamespaceURI() == null ? "" : r.getNamespaceURI();
          final String value = r.getAttributeValue(null, "value");
          if (ln.equals("div") && !XHTML.equals(ns) && value != null && "text".equals(stack.peek())) {
            final XMLStreamReader inner = IN.createXMLStreamReader(new StringReader(value));
            inner.nextTag();
            copySubtree(inner, w);
            // Skip the placeholder's own end tag.
            r.next();
          } else {
            stack.push(ln);
            start(r, w, null);
          }
        }
        case XMLStreamConstants.END_ELEMENT -> {
          stack.pop();
          w.writeEndElement();
        }
        case XMLStreamConstants.CDATA -> w.writeCData(r.getText());
        case XMLStreamConstants.CHARACTERS, XMLStreamConstants.SPACE ->
            w.writeCharacters(r.getText());
        case XMLStreamConstants.COMMENT -> w.writeComment(r.getText());
        case XMLStreamConstants.PROCESSING_INSTRUCTION ->
            w.writeProcessingInstruction(r.getPITarget(), r.getPIData());
        default -> {}
      }
    }
    w.flush();
    return sw.toString();
  }

  // ---------------------------------------------------------------------------------------------
  // Checks.
  // ---------------------------------------------------------------------------------------------

  static final FhirContext CTX = FhirContext.forR4();
  static final String FHIR = "http://hl7.org/fhir";
  static final String H = "xmlns=\"" + XHTML + "\"";
  static final String STATUS = "<status value=\"generated\"/>";

  static boolean sameResource(String a, String b) {
    final IParser p = CTX.newXmlParser();
    return ((Base) p.parseResource(a)).equalsDeep((Base) p.parseResource(b));
  }

  static String show(String s) {
    return s == null ? "null" : s.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
  }

  static Map<String, String> edges() {
    final Map<String, String> m = new java.util.LinkedHashMap<>();
    final String open = "<Patient xmlns=\"" + FHIR + "\"><id value=\"x\"/><text>" + STATUS;
    final String close = "</text></Patient>";
    m.put("plain", open + "<div " + H + "><p>Jane Smith</p></div>" + close);
    m.put(
        "escaped text, comment, attribute",
        open + "<div lang=\"en\" " + H + "><p>Jane &lt;Smith&gt; &amp; co</p><!-- c --></div>" + close);
    m.put("nested divs", open + "<div " + H + "><div><p>a</p></div><div>b</div></div>" + close);
    m.put("whitespace before </text>", open + "<div " + H + ">a</div>\n   " + close);
    m.put("comment before </text>", open + "<div " + H + ">a</div><!-- x -->" + close);
    m.put(
        "prefixed XHTML",
        open + "<h:div xmlns:h=\"" + XHTML + "\"><h:p>a &amp; b</h:p></h:div>" + close);
    m.put(
        "prefix declared on the root",
        "<Patient xmlns=\"" + FHIR + "\" xmlns:h=\"" + XHTML + "\"><text>" + STATUS
            + "<h:div><h:p>a</h:p></h:div>" + close);
    m.put(
        "CDATA holding </div></text>",
        open + "<div " + H + "><pre><![CDATA[</div></text>]]></pre></div>" + close);
    m.put(
        "char refs, newline, tab, quote",
        open + "<div " + H + ">\n\t<p class=\"x\">&#169; \"q\" 1 &lt; 2 &#x1F600;</p>\n</div>" + close);
    m.put(
        "narrative contains value=\"@@\"",
        open + "<div " + H + "><p title=\"@@NARRATIVE0@@\">a</p></div>" + close);
    m.put(
        "contained resource with a narrative",
        "<Observation xmlns=\"" + FHIR + "\"><contained><Patient><id value=\"c\"/><text>" + STATUS
            + "<div " + H + ">contained</div></text></Patient></contained><status value=\"final\"/>"
            + "<code><text value=\"t\"/></code></Observation>");
    m.put(
        "Bundle with two narratives",
        "<Bundle xmlns=\"" + FHIR + "\"><type value=\"collection\"/>"
            + "<entry><resource><Patient><text>" + STATUS + "<div " + H + "><p>one</p></div></text></Patient></resource></entry>"
            + "<entry><resource><Patient><text>" + STATUS + "<div " + H + "><p>two &amp; 2</p></div></text></Patient></resource></entry>"
            + "</Bundle>");
    m.put(
        "text of an Annotation is not a narrative",
        "<Observation xmlns=\"" + FHIR + "\"><status value=\"final\"/><code><text value=\"t\"/></code>"
            + "<note><text value=\"a note\"/></note></Observation>");
    m.put("no narrative", "<Patient xmlns=\"" + FHIR + "\"><id value=\"y\"/></Patient>");
    return m;
  }

  static void runEdges() throws Exception {
    System.out.println("StAX input:  " + IN.getClass().getName());
    System.out.println("StAX output: " + OUT.getClass().getName());
    int bad = 0;
    for (var e : edges().entrySet()) {
      final Rewritten rw = rewrite(e.getValue());
      final String back = restore(rw.xml);
      String verdict;
      try {
        verdict = sameResource(e.getValue(), back) ? "L2 equal" : "NOT EQUAL";
      } catch (Exception ex) {
        verdict = "ERROR " + ex.getMessage().lines().findFirst().orElse("");
      }
      if (!verdict.equals("L2 equal")) {
        bad++;
      }
      System.out.printf(
          "%-42s narratives=%d  %s%s%n",
          e.getKey(), rw.narratives.size(), verdict, rw.findings.isEmpty() ? "" : "  " + rw.findings);
      for (String n : rw.narratives) {
        System.out.println("      captured: " + show(n));
      }
    }
    System.out.println(bad == 0 ? "ALL EDGE CASES L2 EQUAL" : bad + " EDGE CASES NOT EQUAL");
  }

  static void runCorpus(Path dir) throws Exception {
    final IParser json = CTX.newJsonParser();
    final IParser xml = CTX.newXmlParser();
    xml.setPrettyPrint(false);
    int parityWhitespace = 0;
    int resources = 0, equal = 0, withNarrative = 0, parityChecked = 0, parityEqual = 0, withContained = 0;
    int unparsed = 0;
    long hapiNs = 0, passNs = 0;
    final List<String> failures = new ArrayList<>();
    final List<String> parityFailures = new ArrayList<>();
    final List<Path> files;
    try (Stream<Path> s = Files.list(dir)) {
      files = s.filter(p -> p.toString().endsWith(".ndjson")).sorted().toList();
    }
    for (Path f : files) {
      for (String line : Files.readAllLines(f)) {
        if (line.isBlank()) {
          continue;
        }
        final org.hl7.fhir.instance.model.api.IBaseResource res;
        final String doc;
        try {
          res = json.parseResource(line);
          doc = xml.encodeResourceToString(res);
        } catch (Exception e) {
          unparsed++;
          continue;
        }
        resources++;
        final String name = f.getFileName() + ":" + ((Base) res).getIdBase();
        long t0 = System.nanoTime();
        final Base original = (Base) xml.parseResource(doc);
        hapiNs += System.nanoTime() - t0;
        t0 = System.nanoTime();
        final Rewritten rw = rewrite(doc);
        final String back = restore(rw.xml);
        passNs += System.nanoTime() - t0;
        if (!rw.narratives.isEmpty()) {
          withNarrative++;
        }
        if (!rw.findings.isEmpty()) {
          withContained++;
        }
        if (original.equalsDeep((Base) xml.parseResource(back))) {
          equal++;
        } else {
          failures.add(name);
        }
        // Parity with JSON: the narrative taken from the XML equals the one in the JSON.
        if (res instanceof DomainResource dr
            && dr.hasText()
            && dr.getText().hasDiv()
            && rw.findings.isEmpty()
            && rw.narratives.size() == 1) {
          parityChecked++;
          // The parser returns a document node, whose only child is the div.
          final XhtmlNode fromXml =
              new XhtmlParser().parse(rw.narratives.get(0), "div").getChildNodes().get(0);
          final XhtmlNode fromJson = dr.getText().getDiv();
          if (fromJson.equalsDeep(fromXml)) {
            parityEqual++;
          } else if (fromJson.getValueAsString().replaceAll("\\s+", " ")
              .equals(fromXml.getValueAsString().replaceAll("\\s+", " "))) {
            // HAPI's XML writer, which made the fixture, collapses whitespace runs in a narrative.
            parityWhitespace++;
          } else {
            parityFailures.add(name);
          }
        }
      }
    }
    System.out.printf("resources written to XML by HAPI: %d (unparsed from JSON: %d)%n", resources, unparsed);
    System.out.printf("L2 equal after rewrite + restore:  %d / %d%n", equal, resources);
    System.out.printf("  with a narrative rewritten:      %d%n", withNarrative);
    System.out.printf("  with a contained narrative seen: %d%n", withContained);
    System.out.printf(
        "JSON parity of the narrative:      %d exact, %d differing only in whitespace, %d other, of %d%n",
        parityEqual, parityWhitespace, parityFailures.size(), parityChecked);
    System.out.printf(
        "time, HAPI XML parse: %d ms; rewrite + restore: %d ms%n", hapiNs / 1_000_000, passNs / 1_000_000);
    failures.forEach(x -> System.out.println("  NOT L2 EQUAL: " + x));
    parityFailures.forEach(x -> System.out.println("  PARITY FAILED: " + x));
  }


  // ---------------------------------------------------------------------------------------------
  // Bundles: split in the streaming pass.
  // ---------------------------------------------------------------------------------------------

  /** One entry: the type, full URL, identity and, where the type was asked for, the resource. */
  static final class Entry {
    String type;
    String fullUrl;
    String id;
    String versionId;
    String xml;
  }

  /** The value attribute of a first-level child, and of meta's versionId, of a resource root. */
  static String[] identity(String resourceXml) throws XMLStreamException {
    final XMLStreamReader r = IN.createXMLStreamReader(new StringReader(resourceXml));
    final List<String> path = new ArrayList<>();
    String id = null;
    String version = null;
    while (r.hasNext()) {
      final int ev = r.next();
      if (ev == XMLStreamConstants.START_ELEMENT) {
        path.add(r.getLocalName());
        if (path.size() == 2 && path.get(1).equals("id")) {
          id = r.getAttributeValue(null, "value");
        } else if (path.size() == 3 && path.get(1).equals("meta") && path.get(2).equals("versionId")) {
          version = r.getAttributeValue(null, "value");
        }
      } else if (ev == XMLStreamConstants.END_ELEMENT) {
        path.remove(path.size() - 1);
      }
    }
    return new String[] {id, version};
  }

  /**
   * Splits a bundle into its entries in one streaming pass. The resource is captured, with its
   * narrative rewritten, only for entries of the requested type; the others contribute their
   * identity. A nested bundle is skipped, and an entry without a resource carries no type.
   */
  static List<Entry> split(String xml, String type) throws XMLStreamException {
    final XMLStreamReader r = IN.createXMLStreamReader(new StringReader(xml));
    r.nextTag();
    if (!r.getLocalName().equals("Bundle") || !FHIR.equals(r.getNamespaceURI())) {
      throw new IllegalArgumentException("Expected a bundle and found " + r.getLocalName());
    }
    final List<Entry> out = new ArrayList<>();
    final List<String> p = new ArrayList<>();
    p.add("Bundle");
    Entry cur = null;
    while (r.hasNext()) {
      final int ev = r.next();
      if (ev == XMLStreamConstants.START_ELEMENT) {
        final String ln = r.getLocalName();
        p.add(ln);
        final int n = p.size();
        if (n == 2 && ln.equals("entry")) {
          cur = new Entry();
        } else if (n == 3 && cur != null && ln.equals("fullUrl")) {
          cur.fullUrl = r.getAttributeValue(null, "value");
        } else if (n == 4 && cur != null && p.get(2).equals("resource") && !ln.equals("Bundle")) {
          cur.type = ln;
          if (ln.equals(type)) {
            final StringWriter cap = new StringWriter();
            final XMLStreamWriter cw = OUT.createXMLStreamWriter(cap);
            copySubtree(r, cw);
            cw.flush();
            p.remove(p.size() - 1);
            final Rewritten rw = rewrite(cap.toString());
            cur.xml = rw.xml;
            final String[] ident = identity(cap.toString());
            cur.id = ident[0];
            cur.versionId = ident[1];
          }
        } else if (n == 5 && cur != null && cur.xml == null && cur.type != null && ln.equals("id")
            && p.get(2).equals("resource")) {
          cur.id = r.getAttributeValue(null, "value");
        } else if (n == 6 && cur != null && cur.xml == null && cur.type != null
            && ln.equals("versionId") && p.get(4).equals("meta") && p.get(2).equals("resource")) {
          cur.versionId = r.getAttributeValue(null, "value");
        }
      } else if (ev == XMLStreamConstants.END_ELEMENT) {
        final int n = p.size();
        p.remove(n - 1);
        if (n == 2 && cur != null) {
          if (cur.type != null) {
            out.add(cur);
          }
          cur = null;
        }
      }
    }
    return out;
  }

  /** The reference an entry is known by, as HAPI reports a resolved reference. */
  static String target(Entry e) {
    return e.type + "/" + e.id + (e.versionId == null ? "" : "/_history/" + e.versionId);
  }

  static void resolveWithMap(Base element, Map<String, String> map) {
    if (element instanceof Reference ref) {
      final String v = ref.getReference();
      if (v != null && v.startsWith("urn:") && map.containsKey(v)) {
        ref.setReference(map.get(v));
      }
    }
    element.children().stream()
        .flatMap(c -> c.getValues().stream())
        .filter(Objects::nonNull)
        .forEach(c -> resolveWithMap(c, map));
  }

  /** The prototype: split in the streaming pass, resolve with the map, write as JSON. */
  static List<String> prototype(String bundleXml, String type) throws Exception {
    final List<Entry> entries = split(bundleXml, type);
    final Map<String, String> map = new HashMap<>();
    for (Entry e : entries) {
      if (e.fullUrl != null && e.fullUrl.startsWith("urn:") && e.id != null) {
        map.put(e.fullUrl, target(e));
      }
    }
    final IParser xml = CTX.newXmlParser();
    final IParser json = CTX.newJsonParser();
    final List<String> out = new ArrayList<>();
    for (Entry e : entries) {
      if (e.xml != null) {
        // HAPI stands in for the layout here, so the narrative is put back first.
        final Resource res = (Resource) xml.parseResource(restore(e.xml));
        resolveWithMap(res, map);
        out.add(json.encodeResourceToString(res));
      }
    }
    return out;
  }

  /** The current BundleTransformer's behaviour, copied from its source: HAPI parse and resolve. */
  static List<String> oracle(String bundleXml, String type) {
    final IParser xml = CTX.newXmlParser();
    final IParser json = CTX.newJsonParser();
    final var parsed = xml.parseResource(bundleXml);
    if (!(parsed instanceof Bundle bundle)) {
      throw new IllegalArgumentException("Expected a bundle and found a resource of type " + parsed.fhirType());
    }
    final List<Resource> resources =
        bundle.getEntry().stream()
            .map(Bundle.BundleEntryComponent::getResource)
            .filter(Objects::nonNull)
            .filter(r -> type.equals(r.fhirType()))
            .toList();
    resources.forEach(NarrativeCheck::oracleResolve);
    return resources.stream().map(json::encodeResourceToString).toList();
  }

  static void oracleResolve(Base element) {
    if (element instanceof Reference reference) {
      if (reference.getResource() instanceof Resource target) {
        final String value = reference.getReference();
        if (value != null && value.startsWith("#")) {
          return;
        }
        if (value != null && value.startsWith("urn:") && target.getIdElement().hasValue()) {
          reference.setReference(target.getIdElement().getValue());
        }
        reference.setResource(null);
      }
    }
    element.children().stream()
        .flatMap(c -> c.getValues().stream())
        .filter(Objects::nonNull)
        .forEach(NarrativeCheck::oracleResolve);
  }

  static final String BUNDLE_OPEN = "<Bundle xmlns=\"" + FHIR + "\"><type value=\"collection\"/>";

  static Map<String, String> bundles() {
    final Map<String, String> m = new java.util.LinkedHashMap<>();
    final String div = "<div " + H + "><p>n &amp; m</p></div>";
    final String tx = "<text>" + STATUS + div + "</text>";
    m.put(
        "urn:uuid references, hand-written",
        BUNDLE_OPEN
            + "<entry><fullUrl value=\"urn:uuid:p1\"/><resource><Patient><id value=\"pat1\"/><meta><versionId value=\"2\"/></meta>"
            + tx + "</Patient></resource></entry>"
            + "<entry><fullUrl value=\"urn:uuid:p2\"/><resource><Patient><id value=\"pat2\"/></Patient></resource></entry>"
            + "<entry><fullUrl value=\"urn:uuid:noid\"/><resource><Patient><active value=\"true\"/></Patient></resource></entry>"
            + "<entry><fullUrl value=\"urn:uuid:obs-1\"/><resource><Observation><id value=\"o1\"/>" + tx
            + "<extension url=\"http://x/e\"><valueReference><reference value=\"urn:uuid:p2\"/></valueReference></extension>"
            + "<status value=\"final\"/><code><text value=\"c\"/></code>"
            + "<subject><reference value=\"urn:uuid:p1\"/><display value=\"d\"/></subject>"
            + "<performer><reference value=\"urn:uuid:nowhere\"/></performer>"
            + "<performer><reference value=\"urn:uuid:noid\"/></performer>"
            + "<performer><reference value=\"Practitioner/9\"/></performer>"
            + "<note><authorReference><reference value=\"urn:uuid:p1\"/></authorReference><text value=\"n\"/></note>"
            + "<valueString value=\"urn:uuid:p1\"/>"
            + "</Observation></resource></entry>"
            + "<entry><request><method value=\"DELETE\"/><url value=\"Patient/gone\"/></request></entry>"
            + "<entry><fullUrl value=\"urn:uuid:inner\"/><resource><Bundle><type value=\"collection\"/><entry><resource><Patient><id value=\"in\"/></Patient></resource></entry></Bundle></resource></entry>"
            + "</Bundle>");
    m.put(
        "http full URLs and contained references",
        BUNDLE_OPEN
            + "<entry><fullUrl value=\"http://ex/Patient/a\"/><resource><Patient><id value=\"a\"/></Patient></resource></entry>"
            + "<entry><fullUrl value=\"http://ex/Observation/b\"/><resource><Observation><id value=\"b\"/>"
            + "<contained><Patient><id value=\"c\"/><text>" + STATUS + "<div " + H + ">c</div></text></Patient></contained>"
            + "<status value=\"final\"/><code><text value=\"c\"/></code>"
            + "<subject><reference value=\"http://ex/Patient/a\"/></subject>"
            + "<performer><reference value=\"#c\"/></performer></Observation></resource></entry></Bundle>");
    // HAPI, parsing a bundle, replaces a resource's id with its urn:uuid full URL when the two share
    // the uuid, and the JSON writer then omits it. The prototype keeps the id.
    m.put(
        "KNOWN DIFFERENCE: id equal to the urn:uuid",
        BUNDLE_OPEN
            + "<entry><fullUrl value=\"urn:uuid:abc\"/><resource><Patient><id value=\"abc\"/></Patient></resource></entry>"
            + "<entry><fullUrl value=\"urn:uuid:def\"/><resource><Observation><id value=\"def\"/><status value=\"final\"/>"
            + "<code><text value=\"c\"/></code><subject><reference value=\"urn:uuid:abc\"/></subject></Observation></resource></entry></Bundle>");
    m.put(
        "prefixed namespace",
        "<f:Bundle xmlns:f=\"" + FHIR + "\"><f:type value=\"collection\"/>"
            + "<f:entry><f:fullUrl value=\"urn:uuid:1\"/><f:resource><f:Patient><f:id value=\"x1\"/>"
            + "<f:text><f:status value=\"generated\"/><h:div xmlns:h=\"" + XHTML + "\"><h:p>pre</h:p></h:div></f:text>"
            + "</f:Patient></f:resource></f:entry>"
            + "<f:entry><f:resource><f:Observation><f:status value=\"final\"/><f:code><f:text value=\"t\"/></f:code>"
            + "<f:subject><f:reference value=\"urn:uuid:1\"/></f:subject></f:Observation></f:resource></f:entry></f:Bundle>");
    return m;
  }

  static int compareBundle(String name, String xml, boolean verbose) {
    // The types the bundle carries, as the current transformer sees them.
    final java.util.Set<String> types = new java.util.TreeSet<>();
    ((Bundle) CTX.newXmlParser().parseResource(xml))
        .getEntry().stream()
        .map(Bundle.BundleEntryComponent::getResource)
        .filter(Objects::nonNull)
        .forEach(r -> types.add(r.fhirType()));
    int bad = 0;
    int rows = 0;
    for (String t : types) {
      if (t.equals("Bundle")) {
        continue;
      }
      try {
        final List<String> a = oracle(xml, t);
        final List<String> b = prototype(xml, t);
        rows += a.size();
        if (!a.equals(b)) {
          bad++;
          System.out.println("  DIFFERENT " + name + " type " + t + ": oracle " + a.size() + ", prototype " + b.size());
          for (int i = 0; i < Math.min(a.size(), b.size()); i++) {
            if (!a.get(i).equals(b.get(i))) {
              System.out.println("    oracle:    " + show(a.get(i)).substring(0, Math.min(400, a.get(i).length())));
              System.out.println("    prototype: " + show(b.get(i)).substring(0, Math.min(400, b.get(i).length())));
              break;
            }
          }
        }
      } catch (Exception e) {
        bad++;
        System.out.println("  ERROR " + name + " type " + t + ": " + e);
      }
    }
    if (verbose) {
      System.out.printf("%-44s types=%s rows=%d %s%n", name, types, rows, bad == 0 ? "same" : "DIFFERENT");
    }
    return bad;
  }

  static void runBundles(Path ndjson, Path jsonDir) throws Exception {
    int bad = 0;
    for (var e : bundles().entrySet()) {
      final boolean known = e.getKey().startsWith("KNOWN DIFFERENCE");
      final int b = compareBundle(e.getKey(), e.getValue(), true);
      if (known) {
        System.out.println(b > 0 ? "  (different, as expected)" : "  UNEXPECTEDLY THE SAME");
        bad += b > 0 ? 0 : 1;
      } else {
        bad += b;
      }
    }
    final IParser json = CTX.newJsonParser();
    final IParser xml = CTX.newXmlParser();
    xml.setPrettyPrint(false);
    int n = 0;
    int skipped = 0;
    final List<String> sources = new ArrayList<>();
    for (String line : Files.readAllLines(ndjson)) {
      if (!line.isBlank()) {
        sources.add(line);
      }
    }
    try (Stream<Path> s = Files.list(jsonDir)) {
      for (Path f : s.filter(q -> q.getFileName().toString().startsWith("bundle-") && q.toString().endsWith(".json")).sorted().toList()) {
        sources.add(Files.readString(f));
      }
    }
    int withUrn = 0;
    for (String src : sources) {
      final String doc;
      try {
        final var res = json.parseResource(src);
        if (!(res instanceof Bundle)) {
          skipped++;
          continue;
        }
        doc = xml.encodeResourceToString(res);
      } catch (Exception ex) {
        skipped++;
        continue;
      }
      n++;
      if (doc.contains("urn:uuid") || doc.contains("urn:oid")) {
        withUrn++;
      }
      final String id = ((Bundle) CTX.newXmlParser().parseResource(doc)).getIdElement().getIdPart();
      bad += compareBundle("corpus " + n + " " + id, doc, false);
    }
    System.out.printf("corpus bundles compared: %d (with urn references: %d, skipped: %d)%n", n, withUrn, skipped);
    System.out.println(bad == 0 ? "ALL BUNDLES SAME AS THE CURRENT TRANSFORMER" : bad + " DIFFERENCES");
  }

  /** Writes one tab-separated line per edge case: name, rewritten document, captured narrative, original document. */
  static void runDump(Path file) throws Exception {
    final StringBuilder b = new StringBuilder();
    for (var e : edges().entrySet()) {
      final Rewritten rw = rewrite(e.getValue());
      final String cap = rw.narratives.isEmpty() ? "" : rw.narratives.get(0);
      b.append(e.getKey().replace("\t", " ")).append('\t')
          .append(java.util.Base64.getEncoder().encodeToString(rw.xml.getBytes("UTF-8"))).append('\t')
          .append(java.util.Base64.getEncoder().encodeToString(cap.getBytes("UTF-8"))).append('\t')
          .append(java.util.Base64.getEncoder().encodeToString(e.getValue().getBytes("UTF-8"))).append('\n');
    }
    Files.writeString(file, b.toString());
  }

  /**
   * Checks documents written by Spark's {@code to_xml}: each line is a name, the Spark output and
   * the original document, the last two in Base64. The restored output must be L2 equal.
   */
  static void runVerify(Path file) throws Exception {
    int bad = 0;
    for (String line : Files.readAllLines(file)) {
      final String[] f = line.split("\t");
      final String spark = new String(java.util.Base64.getDecoder().decode(f[1]), "UTF-8");
      final String original = new String(java.util.Base64.getDecoder().decode(f[2]), "UTF-8");
      String verdict;
      try {
        verdict = sameResource(original, restore(spark)) ? "L2 equal" : "NOT EQUAL";
      } catch (Exception e) {
        verdict = "ERROR " + e.getMessage().lines().findFirst().orElse("");
      }
      if (!verdict.equals("L2 equal")) {
        bad++;
      }
      System.out.printf("%-42s %s%n", f[0], verdict);
    }
    System.out.println(bad == 0 ? "ALL SPARK-WRITTEN DOCUMENTS L2 EQUAL" : bad + " NOT EQUAL");
  }

  public static void main(String[] args) throws Exception {
    switch (args[0]) {
      case "edges" -> runEdges();
      case "corpus" -> runCorpus(Path.of(args[1]));
      case "dump" -> runDump(Path.of(args[1]));
      case "verify" -> runVerify(Path.of(args[1]));
      case "bundles" -> runBundles(Path.of(args[1]), Path.of(args[2]));
      default -> throw new IOException("unknown mode");
    }
  }
}
