package au.csiro.pathling.io;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.IOException;
import org.apache.spark.sql.api.java.UDF1;

/**
 * Spike 08 prototype (throwaway), A-variant's document check, which parse_json cannot make: the text is
 * exactly one JSON object, with nothing after it. Returns null where it is, else the reason. A full
 * tokenisation per document; its cost is part of A-variant's.
 */
public final class Spike08JsonDocCheck implements UDF1<String, String> {

  private static final long serialVersionUID = 1L;

  @Override
  public String call(String doc) {
    if (doc == null) {
      return null;
    }
    try (JsonParser p = Spike08Json.FACTORY.createParser(doc)) {
      if (p.nextToken() != JsonToken.START_OBJECT) {
        return "shape";
      }
      p.skipChildren();
      return p.nextToken() == null ? null : "trailing";
    } catch (IOException e) {
      return e.getMessage().startsWith("Duplicate field") ? "duplicate" : "malformed";
    }
  }
}
