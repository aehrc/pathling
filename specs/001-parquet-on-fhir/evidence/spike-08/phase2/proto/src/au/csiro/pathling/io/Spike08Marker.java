package au.csiro.pathling.io;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Iterator;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

/**
 * Spike 08 prototype (throwaway), candidate A+'s pre-pass. It needs no definitions. One Jackson pass per
 * document rewrites it so that Spark's reader loses nothing it would otherwise lose:
 * <ul>
 *   <li>every number becomes a string marked {@code \u001e#}, holding the number's own text, so a
 *       decimal keeps its lexical form and a number can be told from a string;</li>
 *   <li>a string that could be mistaken for another token's text once a column widens (one beginning
 *       with {@code [}, {@code {} or the marker, or equal to {@code true} or {@code false}) is marked
 *       {@code \u001e$};</li>
 *   <li>a member that is {@code null}, {@code {}}, {@code []} or {@code ""} is removed and recorded;</li>
 *   <li>a duplicate key, a top-level array, text after the object and malformed JSON are refused.</li>
 * </ul>
 * Under {@code fail} the first finding throws. Under {@code drop} the findings travel in the document as
 * the reserved key {@code \u001ef} (an array of "path|kind|value"), and a document that cannot be read
 * as one resource becomes {@code {"\u001ed": reason}}.
 */
public final class Spike08Marker {

  static final String NUMBER = "\u001e#";
  static final String STRING = "\u001e$";
  static final String FINDINGS = "\u001ef";
  static final String DOCUMENT = "\u001ed";
  private static final JsonFactory OUT = new JsonFactory();

  private Spike08Marker() {}

  public static Dataset<String> mark(Dataset<String> docs, String policy) {
    final boolean fail = "fail".equals(policy);
    final MapPartitionsFunction<String, String> f = rows -> new Iterator<>() {
      final Spike08Findings findings = new Spike08Findings(fail, "Resource");

      @Override
      public boolean hasNext() {
        return rows.hasNext();
      }

      @Override
      public String next() {
        return markOne(rows.next(), findings);
      }
    };
    return docs.mapPartitions(f, Encoders.STRING());
  }

  static String markOne(String doc, Spike08Findings f) {
    f.reset();
    if (doc == null) {
      return null;
    }
    final StringWriter w = new StringWriter(doc.length() + 64);
    try (JsonParser p = Spike08Json.FACTORY.createParser(doc);
        JsonGenerator g = OUT.createGenerator(w)) {
      final JsonToken first = p.nextToken();
      if (first != JsonToken.START_OBJECT) {
        f.add(null, "shape", "the document is not one JSON object");
        return document("shape");
      }
      g.writeStartObject();
      members(p, g, f, true);
      if (p.nextToken() != null) {
        f.add(null, "trailing", "content after the resource");
        return document("trailing");
      }
      if (!f.found.isEmpty()) {
        g.writeArrayFieldStart(FINDINGS);
        for (final Row r : f.found) {
          g.writeString(r.getString(0) + "|" + r.getString(1) + "|" + r.getString(2));
        }
        g.writeEndArray();
      }
      g.writeEndObject();
    } catch (JsonProcessingException e) {
      final String kind = e.getMessage().startsWith("Duplicate field") ? "duplicate" : "malformed";
      f.add(null, kind, e.getOriginalMessage());
      return document(kind);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return w.toString();
  }

  private static String document(String reason) {
    return "{\"" + DOCUMENT + "\":\"" + reason + "\"}";
  }

  /** Copies the members of an object whose start has been read and written. */
  private static void members(JsonParser p, JsonGenerator g, Spike08Findings f, boolean root)
      throws IOException {
    while (p.nextToken() == JsonToken.FIELD_NAME) {
      final String name = p.currentName();
      final JsonToken v = p.nextToken();
      f.push(name);
      if (root && "id".equals(name) && v == JsonToken.VALUE_STRING) {
        f.resourceId = p.getText();
      }
      value(p, g, f, v, name);
      f.pop();
    }
    if (!root) {
      g.writeEndObject();
    }
  }

  /** Copies one value; a member's name is written only where the value is kept. */
  private static void value(JsonParser p, JsonGenerator g, Spike08Findings f, JsonToken t, String name)
      throws IOException {
    switch (t) {
      case START_OBJECT -> {
        if (p.nextToken() == JsonToken.END_OBJECT) {
          f.add(null, "empty", "{}");
          return;
        }
        if (name != null) {
          g.writeFieldName(name);
        }
        g.writeStartObject();
        // The first member's name has been read; process it, then the rest.
        final String first = p.currentName();
        final JsonToken fv = p.nextToken();
        f.push(first);
        value(p, g, f, fv, first);
        f.pop();
        members(p, g, f, false);
      }
      case START_ARRAY -> {
        JsonToken e = p.nextToken();
        if (e == JsonToken.END_ARRAY) {
          f.add(null, "empty", "[]");
          return;
        }
        if (name != null) {
          g.writeFieldName(name);
        }
        g.writeStartArray();
        int i = 0;
        while (e != JsonToken.END_ARRAY) {
          f.index(i++);
          if (e == JsonToken.VALUE_NULL) {
            // Kept: a null aligns a repeating primitive with its metadata; the generated checks decide.
            g.writeNull();
          } else {
            value(p, g, f, e, null);
          }
          e = p.nextToken();
        }
        f.index(-1);
        g.writeEndArray();
      }
      case VALUE_NULL -> f.add(null, "empty", "null");
      case VALUE_STRING -> {
        final String s = p.getText();
        if (s.isEmpty()) {
          f.add(null, "empty", "\"\"");
          return;
        }
        if (name != null) {
          g.writeFieldName(name);
        }
        final boolean mark = s.charAt(0) == '[' || s.charAt(0) == '{' || s.charAt(0) == '\u001e'
            || "true".equals(s) || "false".equals(s);
        g.writeString(mark ? STRING + s : s);
      }
      case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> {
        if (name != null) {
          g.writeFieldName(name);
        }
        g.writeString(NUMBER + p.getText());
      }
      default -> {
        if (name != null) {
          g.writeFieldName(name);
        }
        g.copyCurrentEvent(p);
      }
    }
  }
}
