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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * One streaming pass over a FHIR resource in XML, from its start tag to its end tag, which notes
 * what the XML routes need to know of it, and optionally writes it out in the form Spark's XML
 * reader reads correctly (ticket 32).
 *
 * <p>Every narrative, an XHTML {@code div} of a {@code text}, is taken out as markup that declares
 * every namespace it uses, and written back as the {@code value} attribute of an empty {@code div}.
 * That includes a narrative of a section, which is stored, and one of a contained resource, which
 * is not but is read all the same. Every other FHIR element is written unprefixed, in the FHIR
 * namespace declared once on the root. What FHIR does not read is left out without a finding, as
 * ingest in {@code trust} may (owner, 2026-10-22): an element in another namespace, which must
 * never be read as FHIR because its name matches, a {@code div} that is not XHTML, text, comments,
 * processing instructions and attributes in a namespace.
 *
 * <p>A scan is used once, on one thread.
 */
final class ResourceScan {

  @Nullable private final XmlMarkupWriter writer;

  @Nonnull private final Set<String> urns = XmlDocuments.orderedSet();

  @Nullable private String id;

  @Nullable private String versionId;

  private boolean needsRewrite;

  /**
   * Creates a scan.
   *
   * @param writing whether the resource is written out as well as checked
   */
  ResourceScan(final boolean writing) {
    this.writer = writing ? new XmlMarkupWriter(false) : null;
  }

  /**
   * Reads the resource a reader is on the start tag of, leaving the reader on its end tag.
   *
   * @param reader the reader
   * @throws XMLStreamException if the resource is not well-formed XML
   * @throws IllegalArgumentException if the resource is not FHIR XML
   */
  void read(@Nonnull final XMLStreamReader reader) throws XMLStreamException {
    final Deque<String> names = new ArrayDeque<>();
    startFhirElement(reader);
    names.push(reader.getLocalName());
    while (!names.isEmpty()) {
      switch (reader.next()) {
        case XMLStreamConstants.START_ELEMENT -> {
          if (XmlDocuments.isNarrative(reader, names.peek())) {
            narrative(reader);
          } else if (XmlDocuments.isFhir(reader)) {
            element(reader, names);
            names.push(reader.getLocalName());
          } else {
            // An element in another namespace is not FHIR, whatever its name, and is left out.
            needsRewrite = true;
            XmlDocuments.skip(reader);
          }
        }
        case XMLStreamConstants.END_ELEMENT -> {
          names.pop();
          if (writer != null) {
            writer.end();
          }
        }
        default -> {
          // Text, comments and processing instructions carry nothing FHIR reads.
        }
      }
    }
  }

  /** Reads the start tag of an element of the resource other than a narrative. */
  private void element(@Nonnull final XMLStreamReader reader, @Nonnull final Deque<String> names) {
    final String name = reader.getLocalName();
    final String parent = names.peek();
    if (names.size() == 1 && "id".equals(name)) {
      id = XmlDocuments.value(reader);
    } else if (names.size() == 2 && "meta".equals(parent) && "versionId".equals(name)) {
      versionId = XmlDocuments.value(reader);
    }
    if (XmlDocuments.isUrnReference(reader)) {
      urns.add(XmlDocuments.value(reader));
    }
    startFhirElement(reader);
  }

  private void startFhirElement(@Nonnull final XMLStreamReader reader) {
    if (!XmlDocuments.prefix(reader).isEmpty()) {
      needsRewrite = true;
    }
    if (writer != null) {
      final Map<QualifiedAttribute, String> attributes = new LinkedHashMap<>();
      XmlMarkupWriter.attributes(reader)
          .forEach(
              (attribute, value) -> {
                if (attribute.namespace().isEmpty()) {
                  attributes.put(attribute, value);
                }
              });
      writer.start("", reader.getLocalName(), XmlDocuments.FHIR_NAMESPACE, Map.of(), attributes);
    }
  }

  /**
   * Reads a narrative, which the reader is on the start tag of, leaving the reader on its end tag,
   * and writes it as the value of an attribute.
   */
  private void narrative(@Nonnull final XMLStreamReader reader) throws XMLStreamException {
    needsRewrite = true;
    // A div that is not XHTML is not a narrative, and is left out. A scan that is not writing has
    // no use for the markup, and skips it too.
    if (writer == null || !XmlDocuments.XHTML_NAMESPACE.equals(XmlDocuments.namespace(reader))) {
      XmlDocuments.skip(reader);
      return;
    }
    final XmlMarkupWriter markup = new XmlMarkupWriter(false);
    markup.copyElement(reader, "");
    final Map<QualifiedAttribute, String> attributes = new LinkedHashMap<>();
    attributes.put(new QualifiedAttribute("", "value", ""), markup.markup());
    writer.start("", "div", XmlDocuments.FHIR_NAMESPACE, Map.of(), attributes);
    writer.end();
  }

  /**
   * Returns whether the resource must be rewritten before Spark can read it correctly.
   *
   * @return whether it must be rewritten
   */
  boolean needsRewrite() {
    return needsRewrite;
  }

  /**
   * Returns the resource as it was written.
   *
   * @return the rewritten document
   * @throws IllegalStateException if the scan was not writing
   */
  @Nonnull
  String document() {
    if (writer == null) {
      throw new IllegalStateException("The scan did not write the resource");
    }
    return writer.markup();
  }

  /**
   * Returns the resource's id, where it has one.
   *
   * @return the id
   */
  @Nonnull
  Optional<String> getId() {
    return Optional.ofNullable(id);
  }

  /**
   * Returns the version its metadata names, where it names one.
   *
   * @return the version
   */
  @Nonnull
  Optional<String> getVersionId() {
    return Optional.ofNullable(versionId);
  }

  /**
   * Returns the URNs the resource's elements named {@code reference} hold, in the order they occur.
   *
   * @return the URNs
   */
  @Nonnull
  Set<String> getUrns() {
    return urns;
  }
}
