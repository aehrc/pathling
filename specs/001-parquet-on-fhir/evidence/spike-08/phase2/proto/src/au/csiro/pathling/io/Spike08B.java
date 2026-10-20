package au.csiro.pathling.io;

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import ca.uhn.fhir.context.FhirContext;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

/**
 * Spike 08 prototype (throwaway), candidate B's entry points, over public Dataset API only. The fit
 * pass is one job: each partition streams its documents and returns the paths it saw, and the driver
 * builds the fitted layout from the definitions and their union. Encode is a mapPartitions that parses
 * each document straight into a stored row; Spark's generic row encoder ({@code Encoders.row}) carries
 * the rows, so there is no expression encoder of our own and no FHIR object. Decode is the reverse.
 */
public final class Spike08B {

  private Spike08B() {}

  /** Formats: json (one resource per row), xml (one resource per row), xml-bundle (one Bundle per row). */
  public static Dataset<Row> fit(Dataset<String> docs, FhirContext context, String type,
      String format, String policy, boolean findingsColumn) {
    final Spike08Def def = Spike08Def.forResource(FhirDefinitionContext.of(context), type);
    final MapPartitionsFunction<String, String> presence = rows -> {
      final Set<String> seen = new HashSet<>();
      while (rows.hasNext()) {
        final String doc = rows.next();
        switch (format) {
          case "json" -> Spike08Json.presence(doc, def, type, seen);
          case "xml" -> Spike08Xml.presence(doc, def, type, seen);
          default -> Spike08Xml.presenceBundle(doc, def, type, seen);
        }
      }
      return List.of(String.join("\n", seen)).iterator();
    };
    final Set<String> paths = new HashSet<>();
    for (final String part : docs.mapPartitions(presence, Encoders.STRING()).collectAsList()) {
      if (!part.isEmpty()) {
        paths.addAll(List.of(part.split("\n")));
      }
    }
    final Spike08Layout layout = Spike08Layout.fitted(def, paths, findingsColumn);
    final boolean fail = "fail".equals(policy);
    final MapPartitionsFunction<String, Row> encode = rows -> new Iterator<>() {
      final Spike08Findings f = findings(fail, type, policy);
      Iterator<Row> pending = List.<Row>of().iterator();

      @Override
      public boolean hasNext() {
        while (!pending.hasNext() && rows.hasNext()) {
          final String doc = rows.next();
          final List<Row> out = new ArrayList<>(1);
          switch (format) {
            case "json" -> add(out, Spike08Json.parse(doc, layout, type, f));
            case "xml" -> add(out, Spike08Xml.parse(doc, layout, type, f));
            default -> out.addAll(Spike08Xml.parseBundle(doc, layout, type, f));
          }
          pending = out.iterator();
        }
        return pending.hasNext();
      }

      @Override
      public Row next() {
        hasNext();
        return pending.next();
      }
    };
    return docs.mapPartitions(encode, Encoders.row(layout.schema));
  }

  /** Policies: fail and drop (decision 133), and none, which checks nothing, for timing reference only. */
  private static Spike08Findings findings(boolean fail, String type, String policy) {
    final Spike08Findings f = new Spike08Findings(fail, type);
    f.off = "none".equals(policy);
    return f;
  }

  private static void add(List<Row> out, Row row) {
    if (row != null) {
      out.add(row);
    }
  }

  /** Decodes stored rows to json or xml text, one document per row. */
  public static Dataset<String> decode(Dataset<Row> stored, FhirContext context, String type,
      String format) {
    final Spike08Def def = Spike08Def.forResource(FhirDefinitionContext.of(context), type);
    final Spike08Layout layout = Spike08Layout.stored(def, stored.schema());
    final boolean json = "json".equals(format);
    final MapPartitionsFunction<Row, String> render = rows -> new Iterator<>() {
      @Override
      public boolean hasNext() {
        return rows.hasNext();
      }

      @Override
      public String next() {
        final Row row = rows.next();
        return json ? Spike08Json.render(row, layout, type) : Spike08Xml.render(row, layout, type);
      }
    };
    return stored.mapPartitions(render, Encoders.STRING());
  }
}
