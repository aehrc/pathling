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
import static org.junit.jupiter.api.Assertions.assertNull;

import jakarta.annotation.Nonnull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests that the plain-form check recognises only documents that need neither rewriting nor a full
 * parse, and leaves everything else, valid or not, to the full parse (ticket 32).
 */
class PlainFhirXmlTest {

  @ParameterizedTest
  @ValueSource(
      strings = {
        "<Patient xmlns=\"http://hl7.org/fhir\"><id value=\"1\"/></Patient>",
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <Patient xmlns="http://hl7.org/fhir">
          <id value="1"/>
          <name><given value="a &amp; b&#xa;&#10;"/></name>
        </Patient>
        """,
        "<Patient xmlns=\"http://hl7.org/fhir\"/>",
        "<Patient xmlns=\"http://hl7.org/fhir\"><extension url=\"http://x\"><valueString"
            + " value=\"y\"/></extension><active id=\"a1\" value=\"true\" /></Patient>"
      })
  void recognisesAPlainDocument(@Nonnull final String document) {
    assertEquals("Patient", PlainFhirXml.read(document)[0]);
  }

  /**
   * A narrative that declares its own namespace is rewritten as its div's value attribute, as a
   * full parse rewrites it, whatever it holds.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {"<p>a &amp; b</p>", "<pre><![CDATA[</div></text>]]></pre>", "line one\nline two"})
  void rewritesANarrativeAsAFullParseWould(@Nonnull final String content) throws Exception {
    final String document =
        "<Patient xmlns=\"http://hl7.org/fhir\"><id value=\"1\"/><text><status"
            + " value=\"generated\"/><div xmlns=\"http://www.w3.org/1999/xhtml\">"
            + content
            + "</div>\n</text><gender value=\"male\"/></Patient>";
    final ResourceScan scan = new ResourceScan(true);
    final javax.xml.stream.XMLStreamReader reader = XmlDocuments.reader(document);
    reader.nextTag();
    scan.read(reader);

    final String[] read = PlainFhirXml.read(document);

    assertEquals("Patient", read[0]);
    assertEquals(
        scan.written().orElseThrow(),
        read[1].replace("</div>\n</text>", "</div></text>").replace("/>\n</text>", "/></text>"));
  }

  /**
   * Where the scan cannot tell a narrative's end for certain, because the parser's offset stands
   * where it stopped reading ahead, the document is read with a full parse instead, and is prepared
   * all the same.
   */
  @ParameterizedTest
  @ValueSource(strings = {"a<!-- c --><br/>", "<div><p>nested</p></div>"})
  void preparesANarrativeTheScanLeavesToTheFullParse(@Nonnull final String content)
      throws Exception {
    final String document =
        "<Patient xmlns=\"http://hl7.org/fhir\"><id value=\"1\"/><text>"
            + "<div xmlns=\"http://www.w3.org/1999/xhtml\">"
            + content
            + "</div></text></Patient>";
    final ResourceScan scan = new ResourceScan(true);
    final javax.xml.stream.XMLStreamReader reader = XmlDocuments.reader(document);
    reader.nextTag();
    scan.read(reader);

    assertEquals(scan.written().orElseThrow(), XmlDocuments.prepare(document, "Patient"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        // Not in the FHIR namespace, or in it under a prefix.
        "<Patient><id value=\"1\"/></Patient>",
        "<f:Patient xmlns:f=\"http://hl7.org/fhir\"><f:id value=\"1\"/></f:Patient>",
        "<Patient xmlns=\"http://hl7.org/fhir\"><x:id xmlns:x=\"urn:x\" value=\"1\"/></Patient>",
        "<Patient xmlns=\"http://hl7.org/fhir\"><id xmlns=\"urn:x\" value=\"1\"/></Patient>",
        // Something after the root.
        "<Patient xmlns=\"http://hl7.org/fhir\"><id value=\"1\"/></Patient>text",
        "<Patient xmlns=\"http://hl7.org/fhir\"></Patient><Patient xmlns=\"http://hl7.org/fhir\"/>",
        // A narrative that does not declare its own namespace, a comment, CDATA or text.
        "<Patient xmlns=\"http://hl7.org/fhir\" xmlns:h=\"http://www.w3.org/1999/xhtml\"><text>"
            + "<h:div>a</h:div></text></Patient>",
        "<Patient xmlns=\"http://hl7.org/fhir\"><text><div>a</div></text></Patient>",
        "<Patient xmlns=\"http://hl7.org/fhir\"><div xmlns=\"http://www.w3.org/1999/xhtml\">"
            + "a</div></Patient>",
        "<Patient xmlns=\"http://hl7.org/fhir\"><!-- c --></Patient>",
        "<Patient xmlns=\"http://hl7.org/fhir\"><id><![CDATA[x]]></id></Patient>",
        "<Patient xmlns=\"http://hl7.org/fhir\"><id>x</id></Patient>",
        // Not well-formed.
        "<Patient xmlns=\"http://hl7.org/fhir\"><id value=\"1\"></Patient>",
        "<Patient xmlns=\"http://hl7.org/fhir\"><id value=\"a<b\"/></Patient>",
        "<Patient xmlns=\"http://hl7.org/fhir\"><id value=\"a&b\"/></Patient>",
        "<Patient xmlns=\"http://hl7.org/fhir\"><id value='1'/></Patient>",
        "<Patient xmlns=\"http://hl7.org/fhir\"><id value=\"1\"/>",
        "<!DOCTYPE Patient><Patient xmlns=\"http://hl7.org/fhir\"/>",
        "{\"resourceType\":\"Patient\"}"
      })
  void leavesAnythingElseToTheFullParse(@Nonnull final String document) {
    assertNull(PlainFhirXml.read(document));
  }
}
