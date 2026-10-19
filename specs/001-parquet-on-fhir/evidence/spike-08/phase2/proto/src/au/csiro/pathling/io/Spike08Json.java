package au.csiro.pathling.io;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

/**
 * Spike 08 prototype (throwaway), candidate B for JSON: a streaming parser and a renderer of our own,
 * driven by the definitions. The parser makes one pass per document with Jackson's tokenizer and builds
 * the stored row directly, checking each value against its datatype as it goes, so every finding is per
 * value. The renderer writes the stored row back, a decimal with the text it was stored as.
 */
final class Spike08Json {

  static final JsonFactory FACTORY = JsonFactory.builder()
      .enable(com.fasterxml.jackson.core.StreamReadFeature.STRICT_DUPLICATE_DETECTION).build();
  private static final JsonFactory PLAIN = new JsonFactory();

  private Spike08Json() {}

  /** Spark's generic row encoder takes an array as a Scala Seq; this wraps a list without copying it. */
  static Object seq(List<?> list) {
    return scala.jdk.javaapi.CollectionConverters.asScala(list);
  }

  // ------------------------------------------------------------------ presence (the fit pass)

  /** Adds the paths of described elements in one document of the type to the set. */
  static void presence(String doc, Spike08Def root, String type, Set<String> out) {
    if (doc == null) {
      return;
    }
    try (JsonParser p = PLAIN.createParser(doc)) {
      if (p.nextToken() != JsonToken.START_OBJECT) {
        return;
      }
      // The type is normally the first key; where it is not, the paths are held back until it is known.
      Set<String> held = null;
      boolean first = true;
      JsonToken t;
      while ((t = p.nextToken()) == JsonToken.FIELD_NAME) {
        final String name = p.currentName();
        p.nextToken();
        if ("resourceType".equals(name)) {
          if (!type.equals(p.getText())) {
            return;
          }
          if (held != null) {
            out.addAll(held);
          }
          held = null;
          first = false;
          continue;
        }
        if (first) {
          held = new HashSet<>();
          first = false;
        }
        field(p, root, name, "", held != null ? held : out);
      }
      if (held != null) {
        out.addAll(held);
      }
    } catch (IOException e) {
      // A document that is not JSON contributes nothing; the encode pass reports it.
    }
  }

  private static void field(JsonParser p, Spike08Def def, String name, String prefix, Set<String> out)
      throws IOException {
    Spike08Def.Field f = def.field(name);
    if (f == null && name.startsWith("_")) {
      final Spike08Def.Field base = def.field(name.substring(1));
      if (base != null && base.prim != null) {
        out.add(prefix + base.name);
      }
      p.skipChildren();
      return;
    }
    if (f == null || f.resourceType) {
      p.skipChildren();
      return;
    }
    final String path = prefix + name;
    out.add(path);
    if (f.prim != null || f.child == null) {
      p.skipChildren();
      return;
    }
    final JsonToken t = p.currentToken();
    if (t == JsonToken.START_OBJECT) {
      object(p, f.child, path + "/", out);
    } else if (t == JsonToken.START_ARRAY) {
      JsonToken e;
      while ((e = p.nextToken()) != JsonToken.END_ARRAY) {
        if (e == JsonToken.START_OBJECT) {
          object(p, f.child, path + "/", out);
        } else {
          p.skipChildren();
        }
      }
    }
  }

  private static void object(JsonParser p, Spike08Def def, String prefix, Set<String> out)
      throws IOException {
    while (p.nextToken() == JsonToken.FIELD_NAME) {
      final String name = p.currentName();
      p.nextToken();
      field(p, def, name, prefix, out);
    }
  }

  // ------------------------------------------------------------------ encode

  /**
   * Parses one document into a stored row, or returns null where the document is of another type or,
   * under drop, could not be read as one resource.
   */
  static Row parse(String doc, Spike08Layout layout, String type, Spike08Findings f) {
    f.reset();
    if (doc == null) {
      return null;
    }
    try (JsonParser p = FACTORY.createParser(doc)) {
      final JsonToken first = p.nextToken();
      if (first != JsonToken.START_OBJECT) {
        f.add(null, "shape", "the document is not one JSON object");
        return null;
      }
      final Object[] values = new Object[layout.size()];
      final Boolean ours = object(p, layout, f, values, true, type);
      if (ours == null) {
        return null;
      }
      if (p.nextToken() != null) {
        f.add(null, "trailing", "content after the resource");
        return null;
      }
      if (layout.findings >= 0) {
        values[layout.findings] = seq(new ArrayList<>(f.found));
      }
      return RowFactory.create(values);
    } catch (JsonProcessingException e) {
      f.add(null, e.getMessage().startsWith("Duplicate field") ? "duplicate" : "malformed",
          e.getOriginalMessage());
      return null;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Fills the values of one object. Returns TRUE where something was stored, FALSE where nothing was,
   * and null at the root where the resource is of another type.
   */
  private static Boolean object(JsonParser p, Spike08Layout layout, Spike08Findings f, Object[] values,
      boolean root, String type) throws IOException {
    boolean any = false;
    int keys = 0;
    List<String> nullArrays = null;
    Set<String> meta = null;
    JsonToken t;
    while ((t = p.nextToken()) == JsonToken.FIELD_NAME) {
      final String name = p.currentName();
      final JsonToken v = p.nextToken();
      keys++;
      final Integer i = layout.index.get(name);
      final Spike08Def.Field fd = i == null ? null : layout.fields[i];
      if (fd == null) {
        if (root && "resourceType".equals(name)) {
          if (!type.equals(p.getText())) {
            return null;
          }
          continue;
        }
        final Spike08Def.Field base = name.startsWith("_") ? layout.def.field(name.substring(1)) : null;
        if (base != null && base.prim != null) {
          // A primitive's id and extensions are not stored before M5; the element is kept present.
          if (meta == null) {
            meta = new HashSet<>();
          }
          meta.add(base.name);
          p.skipChildren();
        } else if (root && "contained".equals(name)) {
          f.add(name, "contained", "not stored");
          p.skipChildren();
        } else {
          f.add(name, "undescribed", text(p, v));
          p.skipChildren();
        }
        continue;
      }
      if (fd.resourceType) {
        if (!type.equals(p.getText())) {
          return null;
        }
        values[i] = type;
        continue;
      }
      f.push(name);
      final Object value;
      if (fd.repeating) {
        if (v != JsonToken.START_ARRAY) {
          f.add(null, v == JsonToken.VALUE_NULL ? "empty" : "shape", text(p, v));
          p.skipChildren();
          value = null;
        } else {
          final List<Object> list = new ArrayList<>();
          boolean nulls = false;
          int n = 0;
          JsonToken e;
          while ((e = p.nextToken()) != JsonToken.END_ARRAY) {
            f.index(n++);
            if (e == JsonToken.VALUE_NULL && fd.prim != null) {
              nulls = true;
              list.add(null);
            } else {
              list.add(single(p, e, fd, layout.children[i], f));
            }
          }
          if (n == 0) {
            f.index(-1);
            f.add(null, "empty", "[]");
          }
          if (nulls) {
            if (nullArrays == null) {
              nullArrays = new ArrayList<>();
            }
            nullArrays.add(name);
          }
          value = n == 0 ? null : seq(list);
        }
      } else if (v == JsonToken.START_ARRAY) {
        f.add(null, "shape", text(p, v));
        p.skipChildren();
        value = null;
      } else {
        value = single(p, v, fd, layout.children[i], f);
      }
      f.pop();
      if (root && "id".equals(name) && value instanceof String s) {
        f.resourceId = s;
      }
      values[i] = value;
      any |= value != null;
    }
    if (nullArrays != null) {
      for (final String name : nullArrays) {
        if (meta == null || !meta.contains(name)) {
          f.add(name, "empty", "null in an array with no metadata to align with");
        }
      }
    }
    if (keys == 0 && !root) {
      f.add(null, "empty", "{}");
    }
    if (root && layout.def.field("resourceType") != null) {
      values[layout.index.get("resourceType")] = type;
    }
    return any || (meta != null && !meta.isEmpty());
  }

  private static Object single(JsonParser p, JsonToken t, Spike08Def.Field fd, Spike08Layout child,
      Spike08Findings f) throws IOException {
    if (t == JsonToken.VALUE_NULL) {
      f.add(null, "empty", "null");
      return null;
    }
    if (fd.prim == null) {
      if (t != JsonToken.START_OBJECT || child == null) {
        f.add(null, "type", text(p, t));
        p.skipChildren();
        return null;
      }
      final Object[] values = new Object[child.size()];
      final Boolean any = object(p, child, f, values, false, null);
      return Boolean.TRUE.equals(any) ? RowFactory.create(values) : null;
    }
    final String prim = fd.prim;
    switch (prim) {
      case "boolean":
        if (t == JsonToken.VALUE_TRUE || t == JsonToken.VALUE_FALSE) {
          return t == JsonToken.VALUE_TRUE;
        }
        break;
      case "decimal":
        if (t == JsonToken.VALUE_NUMBER_INT || t == JsonToken.VALUE_NUMBER_FLOAT) {
          // The token's own text, so the decimal keeps its lexical form.
          return p.getText();
        }
        break;
      case "integer":
      case "unsignedInt":
      case "positiveInt":
      case "integer64":
        if (t == JsonToken.VALUE_NUMBER_INT) {
          final Object v = Spike08Checks.integer(prim, p.getText());
          if (v == null) {
            f.add(null, "invalid", p.getText());
          }
          return v;
        }
        break;
      default:
        if (t == JsonToken.VALUE_STRING) {
          final String s = p.getText();
          final String kind = Spike08Checks.checkText(prim, s);
          if (kind != null) {
            f.add(null, kind, s);
            return null;
          }
          return "base64Binary".equals(prim) ? Spike08Checks.base64(s) : s;
        }
    }
    f.add(null, "type", text(p, t));
    p.skipChildren();
    return null;
  }

  private static String text(JsonParser p, JsonToken t) throws IOException {
    return t == JsonToken.START_OBJECT ? "{...}" : t == JsonToken.START_ARRAY ? "[...]" : p.getText();
  }

  // ------------------------------------------------------------------ render

  static String render(Row row, Spike08Layout layout, String type) {
    final StringWriter w = new StringWriter(512);
    try (JsonGenerator g = PLAIN.createGenerator(w)) {
      g.writeStartObject();
      g.writeStringField("resourceType", type);
      fields(g, row, layout);
      g.writeEndObject();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return w.toString();
  }

  private static void fields(JsonGenerator g, Row row, Spike08Layout layout) throws IOException {
    for (int i = 0; i < layout.fields.length; i++) {
      final Spike08Def.Field fd = layout.fields[i];
      if (fd == null || fd.resourceType || row.isNullAt(i)) {
        continue;
      }
      if (fd.repeating) {
        final List<Object> list = row.getList(i);
        if (!anyContent(list, fd, layout.children[i])) {
          continue;
        }
        g.writeArrayFieldStart(fd.name);
        for (final Object o : list) {
          if (hasContent(o, fd, layout.children[i])) {
            value(g, o, fd, layout.children[i]);
          }
        }
        g.writeEndArray();
      } else {
        final Object o = row.get(i);
        if (hasContent(o, fd, layout.children[i])) {
          g.writeFieldName(fd.name);
          value(g, o, fd, layout.children[i]);
        }
      }
    }
  }

  private static void value(JsonGenerator g, Object o, Spike08Def.Field fd, Spike08Layout child)
      throws IOException {
    if (fd.prim == null) {
      g.writeStartObject();
      fields(g, (Row) o, child);
      g.writeEndObject();
      return;
    }
    switch (fd.prim) {
      case "decimal" -> g.writeNumber((String) o);
      case "integer", "unsignedInt", "positiveInt" -> g.writeNumber((Integer) o);
      case "integer64" -> g.writeNumber((Long) o);
      case "boolean" -> g.writeBoolean((Boolean) o);
      case "base64Binary" -> g.writeString(Base64.getEncoder().encodeToString((byte[]) o));
      default -> g.writeString((String) o);
    }
  }

  /** FR-019: an element is written only where something beneath it is. */
  static boolean hasContent(Object o, Spike08Def.Field fd, Spike08Layout child) {
    if (o == null) {
      return false;
    }
    if (fd.prim != null) {
      return true;
    }
    final Row row = (Row) o;
    for (int i = 0; i < child.fields.length; i++) {
      final Spike08Def.Field c = child.fields[i];
      if (c == null || c.resourceType || row.isNullAt(i)) {
        continue;
      }
      if (c.repeating ? anyContent(row.getList(i), c, child.children[i])
          : hasContent(row.get(i), c, child.children[i])) {
        return true;
      }
    }
    return false;
  }

  static boolean anyContent(List<Object> list, Spike08Def.Field fd, Spike08Layout child) {
    for (final Object o : list) {
      if (hasContent(o, fd, child)) {
        return true;
      }
    }
    return false;
  }
}
