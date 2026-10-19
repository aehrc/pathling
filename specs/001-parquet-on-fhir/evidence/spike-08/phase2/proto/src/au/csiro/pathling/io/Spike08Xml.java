package au.csiro.pathling.io;

import com.ctc.wstx.stax.WstxInputFactory;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.codehaus.stax2.XMLStreamReader2;

/**
 * Spike 08 prototype (throwaway), candidate B for XML: a StAX parser on the same definitions-driven core
 * as the JSON parser, and a renderer. Each value is the text of a {@code value} attribute, checked
 * against its datatype. The narrative is copied from the document by the parser's own character
 * offsets, so it is exact whatever it holds. Bundles are read in one pass: every entry's fullUrl and
 * id are collected, the requested type's resources are parsed, and their urn: references are patched
 * at the end (FR-062).
 */
final class Spike08Xml {

  static final String FHIR = "http://hl7.org/fhir";
  static final String XHTML = "http://www.w3.org/1999/xhtml";
  private static final XMLInputFactory FACTORY = factory();

  private Spike08Xml() {}

  private static XMLInputFactory factory() {
    final XMLInputFactory f = new WstxInputFactory();
    f.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, true);
    f.setProperty(XMLInputFactory.IS_COALESCING, true);
    f.setProperty(XMLInputFactory.SUPPORT_DTD, false);
    f.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
    return f;
  }

  static XMLStreamReader2 reader(String doc) throws XMLStreamException {
    return (XMLStreamReader2) FACTORY.createXMLStreamReader(new StringReader(doc));
  }

  /** Moves to the next start or end element, returning its event, or -1 at the end of the document. */
  private static int next(XMLStreamReader2 r, Spike08Findings f) throws XMLStreamException {
    while (r.hasNext()) {
      final int e = r.next();
      if (e == XMLStreamConstants.START_ELEMENT || e == XMLStreamConstants.END_ELEMENT) {
        return e;
      }
      if (e == XMLStreamConstants.CHARACTERS && f != null && !r.isWhiteSpace()) {
        f.add(null, "undescribed", "text content: " + r.getText().trim());
      }
    }
    return -1;
  }

  /** Skips the rest of the current element, including its end tag. */
  private static void skip(XMLStreamReader2 r) throws XMLStreamException {
    int depth = 1;
    while (depth > 0) {
      final int e = r.next();
      if (e == XMLStreamConstants.START_ELEMENT) {
        depth++;
      } else if (e == XMLStreamConstants.END_ELEMENT) {
        depth--;
      }
    }
  }

  // ------------------------------------------------------------------ presence (the fit pass)

  static void presence(String doc, Spike08Def root, String type, Set<String> out) {
    if (doc == null) {
      return;
    }
    try {
      final XMLStreamReader2 r = reader(doc);
      if (next(r, null) != XMLStreamConstants.START_ELEMENT || !type.equals(r.getLocalName())) {
        return;
      }
      presenceChildren(r, root, "", out);
    } catch (XMLStreamException | RuntimeException e) {
      // The encode pass reports a document that cannot be read.
    }
  }

  /** Presence of the requested type's resources in a Bundle. */
  static void presenceBundle(String doc, Spike08Def root, String type, Set<String> out) {
    if (doc == null) {
      return;
    }
    try {
      final XMLStreamReader2 r = reader(doc);
      if (next(r, null) != XMLStreamConstants.START_ELEMENT) {
        return;
      }
      while (next(r, null) == XMLStreamConstants.START_ELEMENT) {
        if (!"entry".equals(r.getLocalName())) {
          skip(r);
          continue;
        }
        while (next(r, null) == XMLStreamConstants.START_ELEMENT) {
          if (!"resource".equals(r.getLocalName())) {
            skip(r);
            continue;
          }
          if (next(r, null) == XMLStreamConstants.START_ELEMENT) {
            if (type.equals(r.getLocalName())) {
              presenceChildren(r, root, "", out);
            } else {
              skip(r);
            }
            next(r, null);
          }
        }
      }
    } catch (XMLStreamException | RuntimeException e) {
      // The encode pass reports a Bundle that cannot be read.
    }
  }

  static void presenceChildren(XMLStreamReader2 r, Spike08Def def, String prefix, Set<String> out)
      throws XMLStreamException {
    for (int a = 0; a < r.getAttributeCount(); a++) {
      final Spike08Def.Field f = def.field(r.getAttributeLocalName(a));
      if (f != null && def.isAttribute(f)) {
        out.add(prefix + f.name);
      }
    }
    while (next(r, null) == XMLStreamConstants.START_ELEMENT) {
      final Spike08Def.Field f = def.field(r.getLocalName());
      if (f == null || def.isAttribute(f)) {
        skip(r);
        continue;
      }
      final String path = prefix + f.name;
      out.add(path);
      if (f.prim != null || f.child == null) {
        skip(r);
      } else {
        presenceChildren(r, f.child, path + "/", out);
      }
    }
  }

  // ------------------------------------------------------------------ encode

  /** Parses a document holding one resource, or returns null where it is of another type or dropped. */
  static Row parse(String doc, Spike08Layout layout, String type, Spike08Findings f) {
    f.reset();
    if (doc == null) {
      return null;
    }
    try {
      final XMLStreamReader2 r = reader(doc);
      if (next(r, f) != XMLStreamConstants.START_ELEMENT) {
        f.add(null, "malformed", "no root element");
        return null;
      }
      if (!type.equals(r.getLocalName())) {
        return null;
      }
      if (!FHIR.equals(r.getNamespaceURI())) {
        f.add(null, "namespace", "root namespace " + r.getNamespaceURI());
        return null;
      }
      final Object[] values = new Object[layout.size()];
      element(r, doc, layout, f, values, true, null);
      values[layout.index.get("resourceType")] = type;
      // Woodstox refuses a second root or text after the root as it reads on.
      while (r.hasNext()) {
        r.next();
      }
      if (layout.findings >= 0) {
        values[layout.findings] = Spike08Json.seq(new ArrayList<>(f.found));
      }
      return RowFactory.create(values);
    } catch (XMLStreamException e) {
      f.add(null, "malformed", e.getMessage());
      return null;
    }
  }

  /** Bundle: the resources of the type, each with urn: references resolved. */
  static List<Row> parseBundle(String doc, Spike08Layout layout, String type, Spike08Findings f) {
    f.reset();
    final List<Row> rows = new ArrayList<>();
    if (doc == null) {
      return rows;
    }
    final Map<String, String> targets = new HashMap<>();
    final List<RefHolder> holders = new ArrayList<>();
    try {
      final XMLStreamReader2 r = reader(doc);
      if (next(r, f) != XMLStreamConstants.START_ELEMENT || !"Bundle".equals(r.getLocalName())) {
        f.add(null, "shape", "not a Bundle");
        return rows;
      }
      while (next(r, null) == XMLStreamConstants.START_ELEMENT) {
        if (!"entry".equals(r.getLocalName())) {
          skip(r);
          continue;
        }
        String fullUrl = null;
        String target = null;
        while (next(r, null) == XMLStreamConstants.START_ELEMENT) {
          final String name = r.getLocalName();
          if ("fullUrl".equals(name)) {
            fullUrl = r.getAttributeValue(null, "value");
            skip(r);
          } else if ("resource".equals(name)) {
            if (next(r, null) != XMLStreamConstants.START_ELEMENT) {
              continue;
            }
            final String rt = r.getLocalName();
            if (type.equals(rt)) {
              f.reset();
              final Object[] values = new Object[layout.size()];
              final List<RefHolder> refs = new ArrayList<>();
              element(r, doc, layout, f, values, true, refs);
              values[layout.index.get("resourceType")] = type;
              target = f.resourceId == null ? null : rt + "/" + f.resourceId;
              if (layout.findings >= 0) {
                values[layout.findings] = Spike08Json.seq(new ArrayList<>(f.found));
              }
              rows.add(RowFactory.create(values));
              holders.addAll(refs);
            } else {
              target = otherTarget(r, rt);
            }
            // The end of <resource>.
            next(r, null);
          } else {
            skip(r);
          }
        }
        if (fullUrl != null && fullUrl.startsWith("urn:") && target != null) {
          targets.put(fullUrl, target);
        }
      }
    } catch (XMLStreamException e) {
      f.add(null, "malformed", e.getMessage());
      return new ArrayList<>();
    }
    // FR-062: a reference to another entry by its urn: fullUrl becomes that entry's relative reference.
    resolve(holders, targets);
    return rows;
  }

  /** Reads the id and version of a resource of another type, skipping the rest of it. */
  private static String otherTarget(XMLStreamReader2 r, String rt) throws XMLStreamException {
    String id = null;
    String version = null;
    while (next(r, null) == XMLStreamConstants.START_ELEMENT) {
      final String name = r.getLocalName();
      if ("id".equals(name)) {
        id = r.getAttributeValue(null, "value");
        skip(r);
      } else if ("meta".equals(name)) {
        while (next(r, null) == XMLStreamConstants.START_ELEMENT) {
          if ("versionId".equals(r.getLocalName())) {
            version = r.getAttributeValue(null, "value");
          }
          skip(r);
        }
      } else {
        skip(r);
      }
    }
    return id == null ? null : rt + "/" + id + (version == null ? "" : "/_history/" + version);
  }

  /**
   * Fills the values of the element the reader is on, whose start tag has been read, through its end
   * tag. Returns whether anything was stored. Where refs is given, each stored Reference.reference that
   * starts with urn: is recorded as its holder, with the field's index appended, for resolution.
   */
  private static boolean element(XMLStreamReader2 r, String doc, Spike08Layout layout,
      Spike08Findings f, Object[] values, boolean root, List<RefHolder> refs)
      throws XMLStreamException {
    boolean any = false;
    for (int a = 0; a < r.getAttributeCount(); a++) {
      final String an = r.getAttributeLocalName(a);
      final String ans = r.getAttributeNamespace(a);
      if (ans != null && !ans.isEmpty()) {
        continue;
      }
      final Integer i = layout.index.get(an);
      if (i != null && layout.def.isAttribute(layout.fields[i])) {
        final String v = r.getAttributeValue(a);
        if (v.isEmpty()) {
          f.add(an, "empty", "");
        } else {
          values[i] = v;
          any = true;
        }
      } else if (!(root && "xmlns".equals(an))) {
        f.add("@" + an, "undescribed", r.getAttributeValue(a));
      }
    }
    final Map<Integer, List<Object>> lists = new HashMap<>();
    int children = 0;
    while (next(r, f) == XMLStreamConstants.START_ELEMENT) {
      children++;
      final String name = r.getLocalName();
      final Integer i = layout.index.get(name);
      final Spike08Def.Field fd = i == null ? null : layout.fields[i];
      if (fd == null || layout.def.isAttribute(fd) || fd.resourceType) {
        if (root && "contained".equals(name)) {
          f.add(name, "contained", "not stored");
        } else {
          f.add(name, "undescribed", "<" + name + ">");
        }
        skip(r);
        continue;
      }
      f.push(name);
      final Object value;
      if (XHTML.equals(r.getNamespaceURI()) && "xhtml".equals(fd.prim)) {
        value = narrative(r, doc);
      } else if (!FHIR.equals(r.getNamespaceURI())) {
        f.add(null, "namespace", r.getNamespaceURI());
        skip(r);
        value = null;
      } else if (fd.prim != null) {
        value = primitive(r, fd, f);
      } else {
        final Spike08Layout child = layout.children[i];
        final Object[] cv = new Object[child.size()];
        final boolean got = element(r, doc, child, f, cv, false, refs);
        value = got ? RowFactory.create(cv) : null;
        if (refs != null && "Reference".equals(fd.fhirType) && got) {
          final Integer ri = child.index.get("reference");
          if (ri != null && cv[ri] instanceof String s && s.startsWith("urn:")) {
            // GenericRow keeps the array it is given, so patching the array patches the row.
            refs.add(new RefHolder(cv, ri));
          }
        }
      }
      f.pop();
      if (root && "id".equals(name) && value instanceof String s) {
        f.resourceId = s;
      }
      if (fd.repeating) {
        lists.computeIfAbsent(i, k -> new ArrayList<>()).add(value);
      } else if (values[i] != null) {
        f.add(name, "shape", "a singular element given more than once");
      } else {
        values[i] = value;
      }
      any |= value != null;
    }
    for (final Map.Entry<Integer, List<Object>> e : lists.entrySet()) {
      values[e.getKey()] = Spike08Json.seq(e.getValue());
    }
    if (!any && children == 0 && !root) {
      f.add(null, "empty", "an element with no content");
    }
    return any;
  }

  /** A pointer to one field of a row under construction. */
  record RefHolder(Object[] row, int index) {}

  private static Object primitive(XMLStreamReader2 r, Spike08Def.Field fd, Spike08Findings f)
      throws XMLStreamException {
    String value = null;
    boolean hasValue = false;
    for (int a = 0; a < r.getAttributeCount(); a++) {
      final String an = r.getAttributeLocalName(a);
      if ("value".equals(an)) {
        value = r.getAttributeValue(a);
        hasValue = true;
      } else if (!"id".equals(an)) {
        f.add("@" + an, "undescribed", r.getAttributeValue(a));
      }
    }
    boolean meta = false;
    while (next(r, f) == XMLStreamConstants.START_ELEMENT) {
      if ("extension".equals(r.getLocalName())) {
        meta = true;
      } else {
        f.add(r.getLocalName(), "undescribed", "<" + r.getLocalName() + ">");
      }
      skip(r);
    }
    if (!hasValue) {
      if (!meta) {
        f.add(null, "empty", "no value");
      }
      return null;
    }
    final String kind = Spike08Checks.checkText(fd.prim, value);
    if (kind != null) {
      f.add(null, kind, value);
      return null;
    }
    switch (fd.prim) {
      case "boolean":
        return "true".equals(value);
      case "integer":
      case "unsignedInt":
      case "positiveInt":
      case "integer64":
        return Spike08Checks.integer(fd.prim, value);
      case "base64Binary":
        return Spike08Checks.base64(value);
      default:
        return value;
    }
  }

  /** The narrative's XHTML, copied from the document by offset, its line feed references unescaped. */
  private static String narrative(XMLStreamReader2 r, String doc) throws XMLStreamException {
    final int start = (int) r.getLocationInfo().getStartingCharOffset();
    skip(r);
    final int end = (int) r.getLocationInfo().getEndingCharOffset();
    return doc.substring(start, end).replace("&#xa;", "\n").replace("&#xd;", "\r");
  }

  /** Resolves recorded references after a Bundle has been read. */
  static void resolve(List<RefHolder> holders, Map<String, String> targets) {
    for (final RefHolder rh : holders) {
      final Object v = rh.row()[rh.index()];
      final String resolved = v == null ? null : targets.get((String) v);
      if (resolved != null) {
        rh.row()[rh.index()] = resolved;
      }
    }
  }

  // ------------------------------------------------------------------ render

  static String render(Row row, Spike08Layout layout, String type) {
    final StringBuilder b = new StringBuilder(1024);
    b.append('<').append(type).append(" xmlns=\"").append(FHIR).append('"');
    children(b, row, layout);
    b.append("</").append(type).append('>');
    return b.toString();
  }

  /** Writes the rest of a start tag (attributes, then '>'), then the children. */
  private static void children(StringBuilder b, Row row, Spike08Layout layout) {
    for (int i = 0; i < layout.fields.length; i++) {
      final Spike08Def.Field fd = layout.fields[i];
      if (fd != null && layout.def.isAttribute(fd) && !row.isNullAt(i)) {
        b.append(' ').append(fd.name).append("=\"");
        escape(b, (String) row.get(i));
        b.append('"');
      }
    }
    b.append('>');
    for (int i = 0; i < layout.fields.length; i++) {
      final Spike08Def.Field fd = layout.fields[i];
      if (fd == null || fd.resourceType || layout.def.isAttribute(fd) || row.isNullAt(i)) {
        continue;
      }
      if (fd.repeating) {
        for (final Object o : row.getList(i)) {
          if (Spike08Json.hasContent(o, fd, layout.children[i])) {
            value(b, o, fd, layout.children[i]);
          }
        }
      } else if (Spike08Json.hasContent(row.get(i), fd, layout.children[i])) {
        value(b, row.get(i), fd, layout.children[i]);
      }
    }
  }

  private static void value(StringBuilder b, Object o, Spike08Def.Field fd, Spike08Layout child) {
    if ("xhtml".equals(fd.prim)) {
      b.append((String) o);
      return;
    }
    if (fd.prim != null) {
      b.append('<').append(fd.name).append(" value=\"");
      escape(b, switch (fd.prim) {
        case "boolean" -> String.valueOf(o);
        case "integer", "unsignedInt", "positiveInt", "integer64" -> String.valueOf(o);
        case "base64Binary" -> Base64.getEncoder().encodeToString((byte[]) o);
        default -> (String) o;
      });
      b.append("\"/>");
      return;
    }
    b.append('<').append(fd.name);
    children(b, (Row) o, child);
    b.append("</").append(fd.name).append('>');
  }

  private static void escape(StringBuilder b, String s) {
    for (int i = 0; i < s.length(); i++) {
      final char c = s.charAt(i);
      switch (c) {
        case '&' -> b.append("&amp;");
        case '<' -> b.append("&lt;");
        case '>' -> b.append("&gt;");
        case '"' -> b.append("&quot;");
        case '\n' -> b.append("&#xa;");
        case '\r' -> b.append("&#xd;");
        case '\t' -> b.append("&#x9;");
        default -> b.append(c);
      }
    }
  }
}
