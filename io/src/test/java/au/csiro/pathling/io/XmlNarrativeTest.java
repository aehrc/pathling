/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.DomainResource;
import org.hl7.fhir.r4.model.Resource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests that a narrative read from FHIR XML is written back at L2: the written document and the
 * source parse, with HAPI, to resources that are equal under {@code equalsDeep}, contained
 * resources aside (xml-native.md, option 3b; ticket 32).
 *
 * <p>The documents are the 14 hand-written ones the StAX pass was prototyped against, in {@code
 * NarrativeCheck.java} beside xml-native.md. Four of them defeat the regular expression the {@code
 * from_xml} prototype took the narrative with: a comment before {@code </text>}, a narrative only
 * in a contained resource, a prefixed XHTML {@code div}, and {@code </div></text>} inside a CDATA
 * section.
 */
class XmlNarrativeTest {

  @Nonnull private static final String FHIR = "http://hl7.org/fhir";

  @Nonnull private static final String XHTML = "xmlns=\"http://www.w3.org/1999/xhtml\"";

  @Nonnull private static final String STATUS = "<status value=\"generated\"/>";

  @Nonnull
  private static final String OPEN =
      "<Patient xmlns=\"" + FHIR + "\"><id value=\"x\"/><text>" + STATUS;

  @Nonnull private static final String CLOSE = "</text></Patient>";

  @Nonnull private static final IParser PARSER = FhirContext.forR4Cached().newXmlParser();

  /** The documents of a single resource, by what each tries. */
  @Nonnull
  static Stream<Arguments> documents() {
    final Map<String, String> documents = new LinkedHashMap<>();
    documents.put("plain", OPEN + "<div " + XHTML + "><p>Jane Smith</p></div>" + CLOSE);
    documents.put(
        "escaped text, comment, attribute",
        OPEN
            + "<div lang=\"en\" "
            + XHTML
            + "><p>Jane &lt;Smith&gt; &amp; co</p><!-- c --></div>"
            + CLOSE);
    documents.put(
        "nested divs", OPEN + "<div " + XHTML + "><div><p>a</p></div><div>b</div></div>" + CLOSE);
    documents.put("whitespace before </text>", OPEN + "<div " + XHTML + ">a</div>\n   " + CLOSE);
    documents.put("comment before </text>", OPEN + "<div " + XHTML + ">a</div><!-- x -->" + CLOSE);
    documents.put(
        "prefixed XHTML",
        OPEN
            + "<h:div xmlns:h=\"http://www.w3.org/1999/xhtml\"><h:p>a &amp; b</h:p></h:div>"
            + CLOSE);
    documents.put(
        "prefix declared on the root",
        "<Patient xmlns=\""
            + FHIR
            + "\" xmlns:h=\"http://www.w3.org/1999/xhtml\"><text>"
            + STATUS
            + "<h:div><h:p>a</h:p></h:div>"
            + CLOSE);
    documents.put(
        "CDATA holding </div></text>",
        OPEN + "<div " + XHTML + "><pre><![CDATA[</div></text>]]></pre></div>" + CLOSE);
    documents.put(
        "char refs, newline, tab, quote",
        OPEN
            + "<div "
            + XHTML
            + ">\n\t<p class=\"x\">&#169; \"q\" 1 &lt; 2 &#x1F600;</p>\n</div>"
            + CLOSE);
    documents.put(
        "narrative contains value=\"@@\"",
        OPEN + "<div " + XHTML + "><p title=\"@@NARRATIVE0@@\">a</p></div>" + CLOSE);
    documents.put(
        "contained resource with a narrative",
        "<Observation xmlns=\""
            + FHIR
            + "\"><contained><Patient><id value=\"c\"/><text>"
            + STATUS
            + "<div "
            + XHTML
            + ">contained</div></text></Patient></contained><status value=\"final\"/>"
            + "<code><text value=\"t\"/></code></Observation>");
    documents.put(
        "text of an Annotation is not a narrative",
        "<Observation xmlns=\""
            + FHIR
            + "\"><status value=\"final\"/><code><text value=\"t\"/></code>"
            + "<note><text value=\"a note\"/></note></Observation>");
    documents.put("no narrative", "<Patient xmlns=\"" + FHIR + "\"><id value=\"y\"/></Patient>");
    return documents.entrySet().stream()
        .map(document -> Arguments.of(document.getKey(), document.getValue()));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("documents")
  void writesTheNarrativeBackAtL2(@Nonnull final String name, @Nonnull final String document) {
    final Resource source = (Resource) PARSER.parseResource(document);
    final String type = source.fhirType();

    final List<String> written = roundTrip(type, documents(document));

    assertEquals(1, written.size());
    assertL2Equal(source, written.get(0));
  }

  /** The fourteenth document: a bundle whose two entries carry a narrative each. */
  @Test
  void writesTheNarrativesOfABundleBackAtL2() {
    final String bundle =
        "<Bundle xmlns=\""
            + FHIR
            + "\"><type value=\"collection\"/>"
            + "<entry><resource><Patient><id value=\"1\"/><text>"
            + STATUS
            + "<div "
            + XHTML
            + "><p>one</p></div></text></Patient></resource></entry>"
            + "<entry><resource><Patient><id value=\"2\"/><text>"
            + STATUS
            + "<div "
            + XHTML
            + "><p>two &amp; 2</p></div></text></Patient></resource></entry>"
            + "</Bundle>";
    final List<Resource> sources =
        ((Bundle) PARSER.parseResource(bundle))
            .getEntry().stream().map(Bundle.BundleEntryComponent::getResource).toList();

    final Dataset<Row> stored =
        TransformFixtures.fhirReader().xml().readBundles("Patient", documents(bundle));
    final List<String> written =
        TransformFixtures.fhirWriter().xml().write("Patient", stored.orderBy("id")).collectAsList();

    assertEquals(2, written.size());
    for (int i = 0; i < 2; i++) {
      assertL2Equal(sources.get(i), written.get(i));
    }
  }

  /**
   * The {@code xml} prefix is bound in every document, so an attribute such as {@code xml:lang} is
   * stored and written back with no declaration of it, as JSON stores the same narrative.
   */
  @Test
  void declaresNothingForTheXmlPrefix() {
    final String div = "<div " + XHTML + " xml:lang=\"en\" lang=\"en\"><p>a</p></div>";
    final Dataset<Row> stored =
        TransformFixtures.fhirReader().xml().read("Patient", documents(OPEN + div + CLOSE));

    final String written = TransformFixtures.fhirWriter().xml().write("Patient", stored).first();

    assertEquals(div, stored.first().<Row>getAs("text").getAs("div"));
    assertTrue(written.contains(div), written);
  }

  @Nonnull
  private static List<String> roundTrip(
      @Nonnull final String type, @Nonnull final Dataset<String> documents) {
    final Dataset<Row> stored = TransformFixtures.fhirReader().xml().read(type, documents);
    return TransformFixtures.fhirWriter().xml().write(type, stored).collectAsList();
  }

  /** Asserts that two documents are the same resource, a contained resource aside. */
  private static void assertL2Equal(@Nonnull final Resource source, @Nonnull final String written) {
    final Resource expected = source.copy();
    if (expected instanceof final DomainResource domain) {
      domain.getContained().clear();
    }
    final Resource actual = (Resource) PARSER.parseResource(written);
    assertTrue(
        expected.equalsDeep(actual),
        "not L2 equal:\n" + PARSER.encodeResourceToString(expected) + "\n" + written);
  }

  @Nonnull
  static String messages(@Nonnull final Throwable failure) {
    final StringBuilder messages = new StringBuilder();
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      messages.append(cause.getMessage()).append('\n');
    }
    return messages.toString();
  }

  @Nonnull
  private static Dataset<String> documents(@Nonnull final String... documents) {
    return TransformFixtures.spark().createDataset(List.of(documents), Encoders.STRING());
  }
}
