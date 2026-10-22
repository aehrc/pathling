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
import javax.xml.XMLConstants;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * Writes XML markup as text, declaring every namespace an element or an attribute uses that is not
 * already in scope, so that a subtree copied out of a larger document stands on its own (ticket
 * 32).
 *
 * <p>It is the serialiser of the XML routes: of a narrative taken out of a document on the way in,
 * of a document rewritten so that Spark's XML reader can read it, and of a narrative written back
 * out. An element with no content is written as an empty-element tag. Text is escaped so that it
 * reads back as the same characters, a carriage return included, and an attribute value so that a
 * line feed, carriage return or tab in it survives XML's attribute-value normalisation. In
 * line-delimited mode a line feed in text is written as a character reference too, so that the
 * output holds no line break; a comment, a processing instruction or a CDATA section holding one
 * cannot be written that way and is refused.
 *
 * <p>A writer is used for one document, on one thread.
 */
final class XmlMarkupWriter {

  @Nonnull private final StringBuilder output;

  private final boolean lineDelimited;

  /** The namespace bindings in scope in the output, one map for each open element. */
  @Nonnull private final Deque<Map<String, String>> scopes = new ArrayDeque<>();

  /** The qualified names of the open elements, so that each end tag repeats its start tag. */
  @Nonnull private final Deque<String> open = new ArrayDeque<>();

  /** Whether the last start tag is still waiting for its {@code >}. */
  private boolean pendingStart;

  /**
   * Creates a writer.
   *
   * @param lineDelimited whether a line feed in text is written as a character reference
   */
  XmlMarkupWriter(final boolean lineDelimited) {
    this.output = new StringBuilder(256);
    this.lineDelimited = lineDelimited;
  }

  /**
   * Writes a start tag, declaring the namespaces the element and its attributes use that are not in
   * scope, after any declarations the source made on it.
   *
   * @param prefix the element's prefix, or the empty string
   * @param localName the element's local name
   * @param namespace the element's namespace, or the empty string
   * @param declarations the namespace declarations the source carried on the element, by prefix
   * @param attributes the element's attributes, as qualified name and namespace pairs to values
   */
  void start(
      @Nonnull final String prefix,
      @Nonnull final String localName,
      @Nonnull final String namespace,
      @Nonnull final Map<String, String> declarations,
      @Nonnull final Map<QualifiedAttribute, String> attributes) {
    closePendingStart();
    final Map<String, String> declared = new LinkedHashMap<>(declarations);
    if (!namespace.equals(lookup(declared, prefix))) {
      declared.put(prefix, namespace);
    }
    attributes.keySet().stream()
        .filter(attribute -> !attribute.namespace().isEmpty())
        .filter(attribute -> !attribute.namespace().equals(lookup(declared, attribute.prefix())))
        .forEach(attribute -> declared.put(attribute.prefix(), attribute.namespace()));
    final String name = prefix.isEmpty() ? localName : prefix + ":" + localName;
    output.append('<').append(name);
    declared.forEach(
        (declaredPrefix, uri) -> {
          output.append(declaredPrefix.isEmpty() ? " xmlns" : " xmlns:" + declaredPrefix);
          output.append("=\"");
          escapeAttribute(uri);
          output.append('"');
        });
    attributes.forEach(
        (attribute, value) -> {
          output.append(' ').append(attribute.qualifiedName()).append("=\"");
          escapeAttribute(value);
          output.append('"');
        });
    scopes.push(declared);
    open.push(name);
    pendingStart = true;
  }

  /** Writes the end tag of the element opened last, as an empty-element tag if it held nothing. */
  void end() {
    final String name = open.pop();
    scopes.pop();
    if (pendingStart) {
      output.append("/>");
      pendingStart = false;
    } else {
      output.append("</").append(name).append('>');
    }
  }

  /**
   * Writes text.
   *
   * @param text the characters
   */
  void text(@Nonnull final String text) {
    closePendingStart();
    for (int i = 0; i < text.length(); i++) {
      final char c = text.charAt(i);
      switch (c) {
        case '&' -> output.append("&amp;");
        case '<' -> output.append("&lt;");
        case '>' -> output.append("&gt;");
        // HAPI's writer escapes a quotation mark in text, and so does this, so that a narrative
        // HAPI
        // wrote is written back as it was.
        case '"' -> output.append("&quot;");
        // A parser turns a literal carriage return into a line feed, so one that reached the text
        // was written as a reference, and is written back as one.
        case '\r' -> output.append("&#xd;");
        case '\n' -> output.append(lineDelimited ? "&#xa;" : "\n");
        default -> output.append(c);
      }
    }
  }

  /**
   * Writes markup that is already serialised, as content of the element opened last.
   *
   * @param markup the markup
   */
  void raw(@Nonnull final String markup) {
    closePendingStart();
    output.append(markup);
  }

  /**
   * Writes a CDATA section, which a parser tells apart from the same text escaped.
   *
   * @param text the characters of the section
   */
  void cdata(@Nonnull final String text) {
    refuseLineBreak("a CDATA section", text);
    closePendingStart();
    output.append("<![CDATA[").append(text).append("]]>");
  }

  /**
   * Writes a comment.
   *
   * @param text the text of the comment
   */
  void comment(@Nonnull final String text) {
    refuseLineBreak("a comment", text);
    closePendingStart();
    output.append("<!--").append(text).append("-->");
  }

  /**
   * Writes a processing instruction.
   *
   * @param target the target of the instruction
   * @param data the data of the instruction, or null where it has none
   */
  void processingInstruction(@Nonnull final String target, @Nullable final String data) {
    refuseLineBreak("a processing instruction", target + (data == null ? "" : data));
    closePendingStart();
    output.append("<?").append(target);
    if (data != null && !data.isEmpty()) {
      output.append(' ').append(data);
    }
    output.append("?>");
  }

  /**
   * Copies the element a reader is on, and everything inside it, leaving the reader on the
   * element's end tag. Namespaces in scope in the source but not in the output are declared where
   * they are used. An element in the XHTML namespace is written unprefixed, in the default
   * namespace, as FHIR JSON carries a narrative.
   *
   * @param reader the reader, on a start tag
   * @param defaultNamespace the namespace to give an element that has none, or the empty string to
   *     leave it in none
   * @throws XMLStreamException if the source is not well-formed
   */
  void copyElement(@Nonnull final XMLStreamReader reader, @Nonnull final String defaultNamespace)
      throws XMLStreamException {
    int depth = 0;
    do {
      switch (reader.getEventType()) {
        case XMLStreamConstants.START_ELEMENT -> {
          depth++;
          final String namespace = XmlDocuments.namespace(reader);
          final String prefix = XmlDocuments.prefix(reader);
          final boolean unqualified = namespace.isEmpty() && prefix.isEmpty();
          final String written = unqualified ? defaultNamespace : namespace;
          // XHTML is written in the default namespace, as FHIR JSON carries a narrative, whatever
          // prefix the source gave it.
          start(
              XmlDocuments.XHTML_NAMESPACE.equals(written) ? "" : prefix,
              reader.getLocalName(),
              written,
              declarations(reader, defaultNamespace),
              attributes(reader));
        }
        case XMLStreamConstants.END_ELEMENT -> {
          depth--;
          end();
        }
        case XMLStreamConstants.CHARACTERS, XMLStreamConstants.SPACE -> text(reader.getText());
        case XMLStreamConstants.CDATA -> cdata(reader.getText());
        case XMLStreamConstants.COMMENT -> comment(reader.getText());
        case XMLStreamConstants.PROCESSING_INSTRUCTION ->
            processingInstruction(reader.getPITarget(), reader.getPIData());
        default -> {
          // Nothing else can occur inside an element of a document without a DTD.
        }
      }
      if (depth > 0) {
        reader.next();
      }
    } while (depth > 0);
  }

  /**
   * Returns what has been written.
   *
   * @return the markup
   */
  @Nonnull
  String markup() {
    closePendingStart();
    return output.toString();
  }

  /**
   * Returns the attributes of the element a reader is on, in order, by qualified name.
   *
   * @param reader the reader, on a start tag
   * @return the attributes and their values
   */
  @Nonnull
  static Map<QualifiedAttribute, String> attributes(@Nonnull final XMLStreamReader reader) {
    final Map<QualifiedAttribute, String> attributes = new LinkedHashMap<>();
    for (int i = 0; i < reader.getAttributeCount(); i++) {
      final String prefix = reader.getAttributePrefix(i);
      final String namespace = reader.getAttributeNamespace(i);
      attributes.put(
          new QualifiedAttribute(
              prefix == null ? "" : prefix,
              reader.getAttributeLocalName(i),
              namespace == null ? "" : namespace),
          reader.getAttributeValue(i));
    }
    return attributes;
  }

  /**
   * Returns the namespace declarations the source carried on the element a reader is on, by prefix.
   * A declaration of the empty namespace as the default is replaced with the namespace an
   * unqualified element is given, so that it does not undo that namespace.
   */
  @Nonnull
  private static Map<String, String> declarations(
      @Nonnull final XMLStreamReader reader, @Nonnull final String defaultNamespace) {
    final Map<String, String> declarations = new LinkedHashMap<>();
    for (int i = 0; i < reader.getNamespaceCount(); i++) {
      final String prefix = reader.getNamespacePrefix(i);
      final String uri = reader.getNamespaceURI(i);
      final String declaredPrefix = prefix == null ? "" : prefix;
      final String declaredUri = uri == null ? "" : uri;
      // A prefix bound to XHTML is not needed once XHTML is written in the default namespace.
      if (declaredPrefix.isEmpty() || !XmlDocuments.XHTML_NAMESPACE.equals(declaredUri)) {
        declarations.put(
            declaredPrefix,
            declaredPrefix.isEmpty() && declaredUri.isEmpty() ? defaultNamespace : declaredUri);
      }
    }
    return declarations;
  }

  @Nullable
  private String lookup(@Nonnull final Map<String, String> own, @Nonnull final String prefix) {
    if (own.containsKey(prefix)) {
      return own.get(prefix);
    }
    for (final Map<String, String> scope : scopes) {
      if (scope.containsKey(prefix)) {
        return scope.get(prefix);
      }
    }
    // The xml prefix is bound in every document without being declared.
    if (XMLConstants.XML_NS_PREFIX.equals(prefix)) {
      return XMLConstants.XML_NS_URI;
    }
    // An unprefixed name is in no namespace until a default is declared.
    return prefix.isEmpty() ? "" : null;
  }

  private void closePendingStart() {
    if (pendingStart) {
      output.append('>');
      pendingStart = false;
    }
  }

  private void refuseLineBreak(@Nonnull final String construct, @Nonnull final String text) {
    if (lineDelimited && (text.indexOf('\n') >= 0 || text.indexOf('\r') >= 0)) {
      throw new IllegalArgumentException(
          "A narrative holds a line break inside "
              + construct
              + ", which a line-delimited document cannot carry");
    }
  }

  private void escapeAttribute(@Nonnull final String value) {
    escapeAttribute(output, value);
  }

  /**
   * Appends the value of an attribute, escaped so that it reads back as the same characters.
   *
   * @param output what to append to
   * @param value the value
   */
  static void escapeAttribute(@Nonnull final StringBuilder output, @Nonnull final String value) {
    for (int i = 0; i < value.length(); i++) {
      final char c = value.charAt(i);
      switch (c) {
        case '&' -> output.append("&amp;");
        case '<' -> output.append("&lt;");
        case '>' -> output.append("&gt;");
        case '"' -> output.append("&quot;");
        // XML normalises these to a space within an attribute value, so they must be references.
        case '\n' -> output.append("&#xa;");
        case '\r' -> output.append("&#xd;");
        case '\t' -> output.append("&#x9;");
        default -> output.append(c);
      }
    }
  }
}
