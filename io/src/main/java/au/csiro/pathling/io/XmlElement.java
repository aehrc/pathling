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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * An element of a document the XML writer wrote, held whole so that the elements left with nothing
 * in them can be removed and each narrative put back as markup (ticket 32). Such a document has
 * FHIR elements and attributes, and text only in a narrative's {@code div}.
 */
final class XmlElement {

  @Nonnull private final String localName;

  @Nonnull private final String namespace;

  @Nonnull private final Map<QualifiedAttribute, String> attributes;

  /** The element's content: elements, and strings for its text. */
  @Nonnull private final List<Object> content;

  private XmlElement(
      @Nonnull final String localName,
      @Nonnull final String namespace,
      @Nonnull final Map<QualifiedAttribute, String> attributes,
      @Nonnull final List<Object> content) {
    this.localName = localName;
    this.namespace = namespace;
    this.attributes = attributes;
    this.content = content;
  }

  /**
   * Reads the element a reader is on the start tag of, leaving the reader on its end tag.
   *
   * @param reader the reader
   * @return the element
   * @throws XMLStreamException if the element is not well-formed
   */
  @Nonnull
  static XmlElement read(@Nonnull final XMLStreamReader reader) throws XMLStreamException {
    final XmlElement element =
        new XmlElement(
            reader.getLocalName(),
            XmlDocuments.namespace(reader),
            XmlMarkupWriter.attributes(reader),
            new ArrayList<>());
    while (true) {
      switch (reader.next()) {
        case XMLStreamConstants.START_ELEMENT -> element.content.add(read(reader));
        case XMLStreamConstants.END_ELEMENT -> {
          return element;
        }
        case XMLStreamConstants.CHARACTERS, XMLStreamConstants.CDATA, XMLStreamConstants.SPACE ->
            element.content.add(reader.getText());
        default -> {
          // Nothing else is written.
        }
      }
    }
  }

  /**
   * Returns this element without the elements beneath it that hold nothing once their own empty
   * elements are removed. This element is kept whatever it holds.
   *
   * @return the element
   */
  @Nonnull
  XmlElement withoutEmptyChildren() {
    final List<Object> kept =
        content.stream()
            .flatMap(
                item ->
                    item instanceof final XmlElement child
                        ? child.withoutEmptyElements().stream().map(Object.class::cast)
                        : java.util.stream.Stream.of(item))
            .toList();
    return new XmlElement(localName, namespace, attributes, kept);
  }

  /**
   * Returns this element without its empty elements, or empty where it holds nothing itself.
   *
   * @return the element, if anything is left of it
   */
  @Nonnull
  Optional<XmlElement> withoutEmptyElements() {
    final XmlElement pruned = withoutEmptyChildren();
    return pruned.attributes.isEmpty() && pruned.content.isEmpty()
        ? Optional.empty()
        : Optional.of(pruned);
  }

  /**
   * Returns the element as markup, with the text of each narrative's {@code div} replaced by the
   * markup it holds.
   *
   * @param narrative the markup a narrative's text holds
   * @param lineDelimited whether a line feed in text is written as a character reference
   * @return the markup
   */
  @Nonnull
  String markup(@Nonnull final UnaryOperator<String> narrative, final boolean lineDelimited) {
    final XmlMarkupWriter writer = new XmlMarkupWriter(lineDelimited);
    write(writer, null, narrative);
    return writer.markup();
  }

  private void write(
      @Nonnull final XmlMarkupWriter writer,
      @Nullable final String parent,
      @Nonnull final UnaryOperator<String> narrative) {
    if ("div".equals(localName) && "text".equals(parent)) {
      writer.raw(
          narrative.apply(
              content.stream()
                  .filter(String.class::isInstance)
                  .map(String.class::cast)
                  .collect(Collectors.joining())));
      return;
    }
    writer.start("", localName, namespace, Map.of(), attributes);
    content.forEach(
        item -> {
          if (item instanceof final XmlElement child) {
            child.write(writer, localName, narrative);
          } else {
            writer.text((String) item);
          }
        });
    writer.end();
  }
}
