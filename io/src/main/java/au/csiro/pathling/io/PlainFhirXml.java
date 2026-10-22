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

/**
 * Recognises, in one pass over its characters, a FHIR XML document in the plain form most writers
 * produce, which Spark's XML reader reads correctly once its narratives are rewritten (ticket 32).
 *
 * <p>The form is: an optional XML declaration, then one root element declaring the FHIR namespace
 * as its default and no other attribute, then elements with unprefixed names and double-quoted,
 * unprefixed attributes, nested correctly, with nothing but whitespace between them and nothing but
 * whitespace after the root. It holds no comment, processing instruction, CDATA section, document
 * type declaration, text or namespace declaration, and every reference in an attribute is one XML
 * defines. A narrative, the {@code div} of a {@code text}, is allowed where it is unprefixed and
 * declares the XHTML namespace itself: it alone is read with a full parse, and written as the
 * {@code value} attribute of its {@code div}, as {@link ResourceScan} writes it. A document in this
 * form needs no other rewriting. It is assumed to be well-formed rather than checked to be: an
 * attribute given twice, or a reference to a character XML does not allow, is passed on as it is.
 * Ingest trusts its input (decision 133's addendum of 2026-10-22), and Spark's XML reader may then
 * fail on such a document without naming it.
 *
 * <p>Anything else is not judged here: the caller reads it with a full parse, which accepts what is
 * valid in another form and names the document where it is not. So this recognises a subset of
 * valid documents, and the cost of a full parse is paid only outside it.
 */
final class PlainFhirXml {

  @Nonnull private static final String DECLARATION = "<?xml";

  @Nonnull private static final String NAMESPACE = " xmlns=\"" + XmlDocuments.FHIR_NAMESPACE + "\"";

  private PlainFhirXml() {}

  /**
   * Returns the name of a document's root, and the document in the form Spark's XML reader reads
   * correctly, where the document is in the plain form with narratives allowed: each narrative, an
   * XHTML {@code div} declaring its own namespace, is written as the {@code value} attribute of its
   * {@code div}, as a full parse would write it.
   *
   * @param document the document
   * @return the name of its root and the document, or null where the document needs a full parse
   */
  @Nullable
  static String[] read(@Nonnull final String document) {
    final StringBuilder prepared = new StringBuilder();
    final int[] copied = {0};
    final String root = scan(document, prepared, copied);
    if (root == null) {
      return null;
    }
    if (copied[0] == 0) {
      return new String[] {root, document};
    }
    return new String[] {root, prepared.append(document, copied[0], document.length()).toString()};
  }

  /**
   * Scans a document, appending to the prepared copy each stretch up to a narrative and the
   * narrative rewritten, and recording how far the document has been copied.
   */
  @Nullable
  private static String scan(
      @Nonnull final String document,
      @Nonnull final StringBuilder prepared,
      @Nonnull final int[] copied) {
    int i = skipWhitespace(document, 0);
    if (document.startsWith(DECLARATION, i)) {
      final int end = document.indexOf("?>", i);
      if (end < 0) {
        return null;
      }
      i = skipWhitespace(document, end + 2);
    }
    if (i >= document.length() || document.charAt(i) != '<') {
      return null;
    }
    final int nameEnd = nameEnd(document, i + 1);
    if (nameEnd == i + 1 || !document.startsWith(NAMESPACE, nameEnd)) {
      return null;
    }
    final String root = document.substring(i + 1, nameEnd);
    i = nameEnd + NAMESPACE.length();
    final Deque<String> open = new ArrayDeque<>();
    // The rest of the root's start tag, which may hold nothing more.
    i = skipWhitespace(document, i);
    if (document.startsWith("/>", i)) {
      return skipWhitespace(document, i + 2) == document.length() ? root : null;
    }
    if (i >= document.length() || document.charAt(i) != '>') {
      return null;
    }
    open.push(root);
    i++;
    while (!open.isEmpty()) {
      i = skipWhitespace(document, i);
      if (i >= document.length() || document.charAt(i) != '<') {
        // Text, or the end of the document before the root is closed.
        return null;
      }
      i =
          i + 1 < document.length() && document.charAt(i + 1) == '/'
              ? endTag(document, i + 2, open)
              : startTag(document, i + 1, open, prepared, copied);
      if (i < 0) {
        return null;
      }
    }
    return skipWhitespace(document, i) == document.length() ? root : null;
  }

  /** Reads an end tag after its {@code </}, returning the index after it, or -1. */
  private static int endTag(
      @Nonnull final String document, final int start, @Nonnull final Deque<String> open) {
    final int nameEnd = nameEnd(document, start);
    if (!document.regionMatches(start, open.peek(), 0, nameEnd - start)
        || nameEnd - start != open.peek().length()) {
      return -1;
    }
    final int close = skipWhitespace(document, nameEnd);
    if (close >= document.length() || document.charAt(close) != '>') {
      return -1;
    }
    open.pop();
    return close + 1;
  }

  /** Reads a start tag after its {@code <}, returning the index after it, or -1. */
  private static int startTag(
      @Nonnull final String document,
      final int start,
      @Nonnull final Deque<String> open,
      @Nonnull final StringBuilder prepared,
      @Nonnull final int[] copied) {
    final int nameEnd = nameEnd(document, start);
    // A comment, an instruction or a prefixed name is never plain.
    if (nameEnd == start) {
      return -1;
    }
    if (document.startsWith("div", start) && nameEnd == start + 3) {
      return "text".equals(open.peek()) ? narrative(document, start - 1, prepared, copied) : -1;
    }
    int i = nameEnd;
    while (true) {
      final int next = skipWhitespace(document, i);
      if (next >= document.length()) {
        return -1;
      }
      final char c = document.charAt(next);
      if (c == '>') {
        open.push(document.substring(start, nameEnd));
        return next + 1;
      }
      if (c == '/') {
        return document.startsWith("/>", next) ? next + 2 : -1;
      }
      // An attribute needs whitespace before it.
      if (next == i) {
        return -1;
      }
      i = attribute(document, next);
      if (i < 0) {
        return -1;
      }
    }
  }

  /**
   * Reads a narrative starting at its {@code <}, appending what precedes it and its rewritten form
   * to the prepared copy, and returning the index after it, or -1 where it needs a full parse.
   */
  private static int narrative(
      @Nonnull final String document,
      final int start,
      @Nonnull final StringBuilder prepared,
      @Nonnull final int[] copied) {
    final String[] narrative = XmlDocuments.narrativeAt(document, start);
    if (narrative == null) {
      return -1;
    }
    final int end = Integer.parseInt(narrative[1]);
    prepared.append(document, copied[0], start).append("<div value=\"");
    XmlMarkupWriter.escapeAttribute(prepared, narrative[0]);
    prepared.append("\"/>");
    copied[0] = end;
    return end;
  }

  /** Reads an unprefixed, double-quoted attribute, returning the index after it, or -1. */
  private static int attribute(@Nonnull final String document, final int start) {
    final int nameEnd = nameEnd(document, start);
    if (nameEnd == start || document.startsWith("xmlns", start) && nameEnd == start + 5) {
      return -1;
    }
    final int equals = skipWhitespace(document, nameEnd);
    if (equals >= document.length() || document.charAt(equals) != '=') {
      return -1;
    }
    final int quote = skipWhitespace(document, equals + 1);
    if (quote >= document.length() || document.charAt(quote) != '"') {
      return -1;
    }
    for (int i = quote + 1; i < document.length(); i++) {
      final char c = document.charAt(i);
      if (c == '"') {
        return i + 1;
      }
      if (c == '<') {
        return -1;
      }
      if (c == '&') {
        i = reference(document, i);
        if (i < 0) {
          return -1;
        }
      }
    }
    return -1;
  }

  /**
   * Checks a reference that starts at an ampersand, returning the index of its semicolon, or -1
   * where it is not one XML defines.
   */
  private static int reference(@Nonnull final String document, final int start) {
    final int end = document.indexOf(';', start);
    if (end < 0 || end - start > 10) {
      return -1;
    }
    final String name = document.substring(start + 1, end);
    final boolean defined =
        switch (name) {
          case "amp", "lt", "gt", "quot", "apos" -> true;
          default -> name.matches("#[0-9]{1,7}|#x[0-9A-Fa-f]{1,6}");
        };
    return defined ? end : -1;
  }

  /** Returns the index after a name of ASCII letters and digits starting with a letter. */
  private static int nameEnd(@Nonnull final String document, final int start) {
    if (start >= document.length() || !isLetter(document.charAt(start))) {
      return start;
    }
    int i = start + 1;
    while (i < document.length()
        && (isLetter(document.charAt(i)) || Character.isDigit(document.charAt(i)))
        && document.charAt(i) < 128) {
      i++;
    }
    return i;
  }

  private static boolean isLetter(final char c) {
    return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z';
  }

  private static int skipWhitespace(@Nonnull final String document, final int start) {
    int i = start;
    while (i < document.length()) {
      final char c = document.charAt(i);
      if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
        break;
      }
      i++;
    }
    return i;
  }
}
