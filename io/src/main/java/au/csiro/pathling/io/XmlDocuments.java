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
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * The passes over FHIR XML documents that Spark's XML reader and writer cannot make themselves,
 * each a streaming parse of one document with the JDK's StAX parser (ticket 32).
 *
 * <p>On the way in, {@link #prepare} checks that a document is one well-formed FHIR resource, and
 * rewrites it where Spark would read it wrongly: a prefixed FHIR namespace is made the default, and
 * each narrative is taken out of the document as markup and put back as the {@code value} attribute
 * of its {@code div}, which Spark reads like any primitive (xml-native.md, option 3b). A document
 * in the plain form most writers produce needs neither, and is recognised without a full parse
 * ({@link PlainFhirXml}). {@link #split} does the same for each resource of one type that a bundle
 * carries, and collects what the bundle's references to its own entries resolve to. On the way out,
 * {@link #finish} removes from what Spark wrote the elements left with nothing in them, and writes
 * each narrative back as XHTML.
 *
 * <p>Ingest is in {@code trust} (owner, 2026-10-22), so a document fails, naming it, only where
 * carrying on would be unsafe or would leave what the data is ambiguous: text that is not
 * well-formed, a second root or text after the root, a document type declaration, and a root
 * outside the FHIR namespace. A document in the plain form is not parsed, so text that is not
 * well-formed in a way that form does not look for is not caught there ({@link PlainFhirXml}).
 * Everything else FHIR does not read is left out without a finding. A document is named by its type
 * and id where they could be read, and by its first characters otherwise.
 *
 * <p>Nothing here builds a FHIR object, and the parser is the JDK's own, so the routes do not
 * depend on which StAX implementation is on the classpath.
 *
 * <p>A failure carries the parser's message but not the parser's exception, which holds a location
 * that cannot be serialised, and so could not be returned from the task that raised it.
 */
final class XmlDocuments {

  /** The namespace of every FHIR element. */
  @Nonnull static final String FHIR_NAMESPACE = "http://hl7.org/fhir";

  /** The namespace of a narrative's XHTML. */
  @Nonnull static final String XHTML_NAMESPACE = "http://www.w3.org/1999/xhtml";

  /** The prefix of a reference that may name another entry of the same bundle. */
  @Nonnull static final String URN = "urn:";

  @Nonnull private static final String BUNDLE = "Bundle";

  @Nonnull private static final String VALUE = "value";

  @Nonnull private static final String TEXT = "text";

  @Nonnull private static final String DIV = "div";

  @Nonnull private static final String REFERENCE = "reference";

  /** An element written with no attributes and no content, which FHIR does not allow. */
  @Nonnull
  static final java.util.regex.Pattern EMPTY_ELEMENT =
      java.util.regex.Pattern.compile("<[A-Za-z][A-Za-z0-9]*/>");

  /** The start and end of a narrative as the XML writer writes it, as the text of a div. */
  @Nonnull private static final String NARRATIVE_START = "<div>";

  @Nonnull private static final String NARRATIVE_END = "</div>";

  /** The StAX property that reports a CDATA section as one rather than as text. */
  @Nonnull
  private static final String REPORT_CDATA =
      "http://java.sun.com/xml/stream/properties/report-cdata-event";

  /** A factory per thread, because a factory is not guaranteed to be safe to share. */
  @Nonnull
  private static final ThreadLocal<XMLInputFactory> FACTORY =
      ThreadLocal.withInitial(XmlDocuments::factory);

  private XmlDocuments() {}

  /**
   * Checks that a document is one FHIR resource and returns it in the form Spark's XML reader reads
   * correctly, or null where it is null, blank or a resource of another type. A blank document is
   * the empty line between two documents of a line-delimited file, and holds nothing to read. The
   * document is returned unchanged where it needs no rewriting, which is the common case.
   *
   * @param document the document
   * @param resourceType the type of the resources wanted
   * @return the document, rewritten where necessary, or null
   * @throws IllegalArgumentException if the document is not well-formed FHIR XML
   */
  @Nullable
  static String prepare(@Nullable final String document, @Nonnull final String resourceType) {
    if (document == null || document.isBlank()) {
      return null;
    }
    // Most documents are in a form that needs no full parse to be checked, and only their
    // narratives rewritten.
    final String[] plain = PlainFhirXml.read(document);
    if (plain != null) {
      return resourceType.equals(plain[0]) ? plain[1] : null;
    }
    // A document that is not plain is usually one with a narrative, so one of the type wanted is
    // written out as it is checked, in one pass, and the copy is used where it differs. One of
    // another type is only checked.
    try {
      final XMLStreamReader reader = root(document);
      final boolean wanted = resourceType.equals(reader.getLocalName());
      final ResourceScan scan = new ResourceScan(wanted);
      scan.read(reader);
      requireEnd(reader);
      if (!wanted) {
        return null;
      }
      return scan.needsRewrite() ? scan.document() : document;
    } catch (final XMLStreamException | IllegalArgumentException e) {
      throw malformed(document, e);
    }
  }

  /**
   * Returns the resources of one type that a bundle carries, each as a document in the form {@link
   * #prepare} returns, beside what each of the URNs it mentions resolves to among the bundle's
   * entries (decision 83, FR-062).
   *
   * <p>A URN resolves to the relative reference of the entry whose full URL it is, with the entry's
   * version where its metadata names one. Only an entry with an id can be resolved to, and where
   * two entries share a full URL the first that can be resolved to is taken. Which of the URNs a
   * resource mentions are the references to resolve is for the definitions to say, so every one
   * that names an entry is given. An entry without a resource contributes nothing, and a bundle
   * carried as an entry's resource is neither returned nor read in turn.
   *
   * @param bundle the bundle
   * @param resourceType the type of the resources wanted
   * @return the resources and their resolutions, in the order the bundle carries them
   * @throws IllegalArgumentException if the bundle is not well-formed FHIR XML, or not a bundle
   */
  @Nonnull
  static List<BundleResource> split(
      @Nullable final String bundle, @Nonnull final String resourceType) {
    if (bundle == null) {
      return List.of();
    }
    try {
      final XMLStreamReader reader = root(bundle);
      if (!BUNDLE.equals(reader.getLocalName())) {
        throw new IllegalArgumentException(
            "Expected a bundle and found a resource of type " + reader.getLocalName());
      }
      final List<BundleEntryScan> entries = entries(reader, resourceType);
      requireEnd(reader);
      return BundleEntryScan.resolve(entries);
    } catch (final XMLStreamException | IllegalArgumentException e) {
      throw malformed(bundle, e);
    }
  }

  /**
   * Writes a stored narrative as the XHTML {@code div} it is, for a document being written. A
   * {@code div} stored without a namespace is written in the XHTML namespace, as FHIR requires.
   *
   * @param div the stored narrative
   * @param name what names the resource in a failure
   * @param lineDelimited whether the document is written on one line
   * @return the narrative's markup
   * @throws IllegalArgumentException if the narrative is not well-formed XML rooted at a {@code
   *     div}, or cannot be written on one line
   */
  @Nullable
  static String narrative(
      @Nullable final String div, @Nullable final String name, final boolean lineDelimited) {
    if (div == null) {
      return null;
    }
    try {
      final XMLStreamReader reader = reader(div);
      reader.nextTag();
      if (!DIV.equals(reader.getLocalName())
          || !(namespace(reader).isEmpty() || XHTML_NAMESPACE.equals(namespace(reader)))) {
        throw new IllegalArgumentException("The narrative is not an XHTML div");
      }
      final XmlMarkupWriter writer = new XmlMarkupWriter(lineDelimited);
      writer.copyElement(reader, XHTML_NAMESPACE);
      requireEnd(reader);
      return writer.markup();
    } catch (final XMLStreamException | IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Cannot write the narrative of "
              + (name == null ? "a resource" : name)
              + ": "
              + e.getMessage());
    }
  }

  /**
   * Finishes a document the XML writer wrote: removes every element that holds nothing, repeatedly,
   * so that an element whose only content was such elements goes too, and replaces the text of each
   * narrative's {@code div} with the XHTML it holds. The XML writer writes a structure whose every
   * field is null as an empty element, which FHIR does not allow, and which the previous writer
   * left out (decision 84); and it can write a narrative only as text.
   *
   * @param document the document, as the XML writer wrote it
   * @param name what names the resource in a failure
   * @param lineDelimited whether the document is written on one line
   * @return the finished document
   * @throws IllegalArgumentException if a narrative is not well-formed XML rooted at a {@code div},
   *     or cannot be written on one line
   */
  @Nullable
  static String finish(
      @Nullable final String document, @Nullable final String name, final boolean lineDelimited) {
    if (document == null) {
      return null;
    }
    // Where nothing is empty, only the narratives change, and they are found without a parse: the
    // XML writer writes a narrative as the escaped text of a div with no attributes, and no other
    // element of that name.
    if (!EMPTY_ELEMENT.matcher(document).find()) {
      return withNarratives(document, name, lineDelimited);
    }
    final XmlElement root;
    try {
      final XMLStreamReader reader = reader(document);
      reader.nextTag();
      root = XmlElement.read(reader).withoutEmptyChildren();
    } catch (final XMLStreamException e) {
      throw new IllegalStateException(
          "The XML writer wrote a document that is not well-formed: " + e.getMessage());
    }
    return root.markup(div -> narrative(div, name, lineDelimited), lineDelimited);
  }

  /**
   * Reads the narrative that starts at an index of a document, an XHTML {@code div} declaring its
   * own namespace, returning its markup and the index just after it, or null where it is not such a
   * narrative or its end cannot be told for certain.
   *
   * @param document the document
   * @param start the index of the narrative's {@code <}
   * @return the markup and the index after the narrative, or null
   */
  @Nullable
  static String[] narrativeAt(@Nonnull final String document, final int start) {
    try {
      final XMLStreamReader reader = reader(document, start);
      reader.nextTag();
      if (!DIV.equals(reader.getLocalName())
          || !XHTML_NAMESPACE.equals(namespace(reader))
          || !prefix(reader).isEmpty()) {
        return null;
      }
      int depth = 1;
      while (depth > 0) {
        final int event = reader.next();
        depth +=
            event == XMLStreamConstants.START_ELEMENT
                ? 1
                : event == XMLStreamConstants.END_ELEMENT ? -1 : 0;
      }
      // The parser's offset is a hint, not a guarantee: it can stand where the parser stopped
      // reading ahead rather than at the end tag. So the span it gives is parsed again on its own,
      // and taken only where it is exactly one element.
      final int end = start + reader.getLocation().getCharacterOffset();
      if (end <= start || end > document.length() || document.charAt(end - 1) != '>') {
        return null;
      }
      final XMLStreamReader span = reader(document.substring(start, end));
      span.nextTag();
      final XmlMarkupWriter writer = new XmlMarkupWriter(false);
      writer.copyElement(span, "");
      requireEnd(span);
      return new String[] {writer.markup(), Integer.toString(end)};
    } catch (final XMLStreamException | IllegalArgumentException e) {
      return null;
    }
  }

  /** Replaces the text of each narrative's div, as the XML writer wrote it, with its markup. */
  @Nonnull
  private static String withNarratives(
      @Nonnull final String document, @Nullable final String name, final boolean lineDelimited) {
    final StringBuilder output = new StringBuilder(document.length() + 64);
    int from = 0;
    int start = document.indexOf(NARRATIVE_START);
    while (start >= 0) {
      final int end = document.indexOf(NARRATIVE_END, start);
      final String text = unescape(document.substring(start + NARRATIVE_START.length(), end));
      output.append(document, from, start).append(narrative(text, name, lineDelimited));
      from = end + NARRATIVE_END.length();
      start = document.indexOf(NARRATIVE_START, from);
    }
    return output.append(document, from, document.length()).toString();
  }

  /** Returns text with the references XML defines replaced by the characters they stand for. */
  @Nonnull
  private static String unescape(@Nonnull final String text) {
    if (text.indexOf('&') < 0) {
      return text;
    }
    final StringBuilder output = new StringBuilder(text.length());
    int i = 0;
    while (i < text.length()) {
      final char c = text.charAt(i);
      final int end = c == '&' ? text.indexOf(';', i) : -1;
      if (end < 0) {
        output.append(c);
        i++;
        continue;
      }
      final String reference = text.substring(i + 1, end);
      switch (reference) {
        case "amp" -> output.append('&');
        case "lt" -> output.append('<');
        case "gt" -> output.append('>');
        case "quot" -> output.append('"');
        case "apos" -> output.append('\'');
        default ->
            output.appendCodePoint(
                reference.startsWith("#x")
                    ? Integer.parseInt(reference.substring(2), 16)
                    : Integer.parseInt(reference.substring(1)));
      }
      i = end + 1;
    }
    return output.toString();
  }

  /**
   * Reads the entries of a bundle the reader is on the root of, capturing the resources of one
   * type, and leaves the reader on the bundle's end tag.
   */
  @Nonnull
  private static List<BundleEntryScan> entries(
      @Nonnull final XMLStreamReader reader, @Nonnull final String resourceType)
      throws XMLStreamException {
    final List<BundleEntryScan> entries = new ArrayList<>();
    while (nextTag(reader) == XMLStreamConstants.START_ELEMENT) {
      if (isFhir(reader) && "entry".equals(reader.getLocalName())) {
        entries.add(entry(reader, resourceType));
      } else {
        skip(reader);
      }
    }
    return entries;
  }

  /** Reads one entry of a bundle, leaving the reader on its end tag. */
  @Nonnull
  private static BundleEntryScan entry(
      @Nonnull final XMLStreamReader reader, @Nonnull final String resourceType)
      throws XMLStreamException {
    final BundleEntryScan entry = new BundleEntryScan();
    while (nextTag(reader) == XMLStreamConstants.START_ELEMENT) {
      final String name = isFhir(reader) ? reader.getLocalName() : "";
      if ("fullUrl".equals(name)) {
        entry.setFullUrl(reader.getAttributeValue(null, VALUE));
        skip(reader);
      } else if ("resource".equals(name)) {
        if (nextTag(reader) == XMLStreamConstants.START_ELEMENT) {
          if (!isFhir(reader)) {
            skip(reader);
            // The end tag of the resource element.
            nextTag(reader);
            continue;
          }
          final String type = reader.getLocalName();
          final boolean wanted = resourceType.equals(type);
          final ResourceScan scan = new ResourceScan(wanted);
          scan.read(reader);
          entry.setResource(type, scan, wanted);
          // The end tag of the resource element.
          nextTag(reader);
        }
      } else {
        skip(reader);
      }
    }
    return entry;
  }

  /**
   * Opens a document and moves to its root, which must be in the FHIR namespace and preceded by
   * nothing but a declaration, comments and processing instructions.
   */
  @Nonnull
  private static XMLStreamReader root(@Nonnull final String document) throws XMLStreamException {
    final XMLStreamReader reader = reader(document);
    while (reader.hasNext()) {
      final int event = reader.next();
      if (event == XMLStreamConstants.START_ELEMENT) {
        requireFhir(reader);
        return reader;
      }
      if (event == XMLStreamConstants.DTD) {
        throw new IllegalArgumentException("A document type declaration is not allowed");
      }
    }
    throw new IllegalArgumentException("The document has no root element");
  }

  /**
   * Reads to the end of a document whose root has been read, failing on a second root or on text
   * after the root.
   */
  private static void requireEnd(@Nonnull final XMLStreamReader reader) throws XMLStreamException {
    while (reader.hasNext()) {
      final int event = reader.next();
      if (event == XMLStreamConstants.START_ELEMENT) {
        throw new IllegalArgumentException("The document has a second root element");
      }
      if ((event == XMLStreamConstants.CHARACTERS || event == XMLStreamConstants.CDATA)
          && !reader.isWhiteSpace()) {
        throw new IllegalArgumentException("The document has text after its root element");
      }
    }
  }

  /**
   * Opens a reader over a document, configured as every pass here reads.
   *
   * @param document the document
   * @return the reader, before the document's first event
   * @throws XMLStreamException if the reader cannot be made
   */
  @Nonnull
  static XMLStreamReader reader(@Nonnull final String document) throws XMLStreamException {
    return FACTORY.get().createXMLStreamReader(new StringReader(document));
  }

  /**
   * Opens a reader over the part of a document from an index, without copying it, so that the
   * reader's character offsets count from that index.
   */
  @Nonnull
  private static XMLStreamReader reader(@Nonnull final String document, final int start)
      throws XMLStreamException {
    final StringReader text = new StringReader(document);
    try {
      text.skip(start);
    } catch (final IOException e) {
      throw new XMLStreamException(e);
    }
    return FACTORY.get().createXMLStreamReader(text);
  }

  /** Moves to the next start or end tag, skipping comments, whitespace and instructions. */
  static int nextTag(@Nonnull final XMLStreamReader reader) throws XMLStreamException {
    while (reader.hasNext()) {
      final int event = reader.next();
      if (event == XMLStreamConstants.START_ELEMENT || event == XMLStreamConstants.END_ELEMENT) {
        return event;
      }
    }
    throw new XMLStreamException("Unexpected end of document");
  }

  /** Skips the element a reader is on the start tag of, leaving it on the element's end tag. */
  static void skip(@Nonnull final XMLStreamReader reader) throws XMLStreamException {
    int depth = 1;
    while (depth > 0) {
      depth += nextTag(reader) == XMLStreamConstants.START_ELEMENT ? 1 : -1;
    }
  }

  /** Returns whether the element a reader is on is in the FHIR namespace. */
  static boolean isFhir(@Nonnull final XMLStreamReader reader) {
    return FHIR_NAMESPACE.equals(namespace(reader));
  }

  /** Fails where the root a reader is on is not in the FHIR namespace. */
  private static void requireFhir(@Nonnull final XMLStreamReader reader) {
    if (!isFhir(reader)) {
      final String namespace = namespace(reader);
      throw new IllegalArgumentException(
          "The element "
              + reader.getLocalName()
              + (namespace.isEmpty() ? " is in no namespace" : " is in the namespace " + namespace)
              + ", not the FHIR namespace");
    }
  }

  /**
   * Returns the namespace of the element a reader is on, or the empty string where it has none.
   *
   * @param reader the reader, on a start or end tag
   * @return the namespace
   */
  @Nonnull
  static String namespace(@Nonnull final XMLStreamReader reader) {
    return Optional.ofNullable(reader.getNamespaceURI()).orElse("");
  }

  /**
   * Returns the prefix of the element a reader is on, or the empty string where it has none.
   *
   * @param reader the reader, on a start or end tag
   * @return the prefix
   */
  @Nonnull
  static String prefix(@Nonnull final XMLStreamReader reader) {
    return Optional.ofNullable(reader.getPrefix()).orElse("");
  }

  /**
   * Returns whether an element is a narrative's {@code div}: named so, under a {@code text}
   * element. It is not the place of a FHIR element of that name, so one in another namespace is a
   * narrative that is not XHTML.
   */
  static boolean isNarrative(
      @Nonnull final XMLStreamReader reader, @Nullable final String parentName) {
    return DIV.equals(reader.getLocalName()) && TEXT.equals(parentName);
  }

  /** Returns whether an element is a reference that may name another entry of the bundle. */
  static boolean isUrnReference(@Nonnull final XMLStreamReader reader) {
    final String value = reader.getAttributeValue(null, VALUE);
    return REFERENCE.equals(reader.getLocalName()) && value != null && value.startsWith(URN);
  }

  /** Returns the value attribute of the element a reader is on, or null where it has none. */
  @Nullable
  static String value(@Nonnull final XMLStreamReader reader) {
    return reader.getAttributeValue(null, VALUE);
  }

  @Nonnull
  private static IllegalArgumentException malformed(
      @Nonnull final String document, @Nonnull final Exception cause) {
    return new IllegalArgumentException(
        "Cannot read FHIR XML " + name(document) + ": " + cause.getMessage());
  }

  /** Names a document by its type and id where they are known, and by its start otherwise. */
  @Nonnull
  private static String name(@Nonnull final String document) {
    final String identified = identify(document);
    if (identified != null) {
      return identified;
    }
    return "document " + DocumentExcerpt.startOf(document);
  }

  /**
   * Reads the type and id of a document as far as it can, so that a failure names the resource
   * rather than quoting it. Nothing is reported from here: a document that cannot be read this far
   * is named by its start.
   */
  @Nullable
  private static String identify(@Nonnull final String document) {
    try {
      final XMLStreamReader reader = reader(document);
      reader.nextTag();
      final String type = reader.getLocalName();
      while (nextTag(reader) == XMLStreamConstants.START_ELEMENT) {
        if ("id".equals(reader.getLocalName()) && value(reader) != null) {
          return type + " with id \"" + value(reader) + "\"";
        }
        skip(reader);
      }
      return null;
    } catch (final XMLStreamException | RuntimeException e) {
      return null;
    }
  }

  @Nonnull
  private static XMLInputFactory factory() {
    final XMLInputFactory factory = XMLInputFactory.newDefaultFactory();
    factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, true);
    factory.setProperty(XMLInputFactory.IS_COALESCING, false);
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
    factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
    if (factory.isPropertySupported(REPORT_CDATA)) {
      factory.setProperty(REPORT_CDATA, true);
    }
    return factory;
  }
}
