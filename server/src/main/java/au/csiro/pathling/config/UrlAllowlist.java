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

package au.csiro.pathling.config;

import jakarta.annotation.Nonnull;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * URL allowlist matching with proper URI semantics. Replaces naive prefix string matching, which is
 * unsound for URLs because it allows host-string confusion attacks (e.g. an allowlist entry {@code
 * https://allowed.example.com} matches the candidate {@code
 * https://allowed.example.com.evil.com/foo}) and userinfo bypasses (e.g. an allowlist entry {@code
 * https://allowed.example.com/} matches the candidate {@code
 * https://allowed.example.com@evil.com/foo}, whose effective host is {@code evil.com}).
 *
 * <p>For an HTTP(S) prefix, a candidate URL is considered allowed when all of the following hold:
 *
 * <ul>
 *   <li>The scheme matches case-insensitively. Only {@code http} and {@code https} are accepted.
 *   <li>The host matches case-insensitively (per RFC 3986 §3.2.2).
 *   <li>The effective port matches, where omitted ports default to 80 for {@code http} and 443 for
 *       {@code https}.
 *   <li>The candidate has no userinfo component.
 *   <li>The candidate path either equals the prefix path or extends it at a path-segment boundary
 *       (i.e. {@code candidatePath.startsWith(prefixPath + "/")}). The empty prefix path and {@code
 *       "/"} are treated as equivalent.
 * </ul>
 *
 * <p>For any other prefix (e.g. {@code file:///staging/}, {@code s3a://bucket/data/} or {@code
 * hdfs://namenode:8020/data/}), the URL is split into scheme, authority and path in the way that
 * Hadoop does, without percent-decoding and without treating {@code ?} or {@code #} as delimiters.
 * A candidate is allowed when all of the following hold:
 *
 * <ul>
 *   <li>The scheme matches case-insensitively.
 *   <li>The authority matches exactly. An absent authority ({@code file:/path}) and an empty one
 *       ({@code file:///path}) are equivalent.
 *   <li>The candidate path contains no {@code ..} segment. The path is resolved by the file system
 *       after this check, so such a segment could lead outside the prefix.
 *   <li>The candidate path contains no {@code \}, <code>{</code> or <code>}</code>. Spark expands
 *       import paths as Hadoop glob patterns, and an escape such as {@code \.\.} or a brace
 *       alternative such as <code>{../other}</code> becomes a {@code ..} segment only after
 *       expansion.
 *   <li>The candidate path equals the prefix path or extends it at a path-segment boundary, as for
 *       HTTP(S).
 * </ul>
 *
 * @author John Grimes
 */
public final class UrlAllowlist {

  /** Splits a URL into scheme, optional authority and path, as Hadoop's {@code Path} does. */
  private static final Pattern URL_PARTS =
      Pattern.compile("^([A-Za-z][A-Za-z0-9+.-]*):(?://([^/]*))?(.*)$", Pattern.DOTALL);

  /** Characters that Hadoop glob expansion can turn into a {@code ..} path segment. */
  private static final String GLOB_TRAVERSAL_CHARACTERS = "\\{}";

  private UrlAllowlist() {
    // Utility class.
  }

  /**
   * Returns true if the candidate URL is allowed by at least one prefix in the list.
   *
   * <p>When {@code allowInsecure} is false, candidate URLs with the {@code http} scheme are
   * rejected outright, regardless of whether any prefix would otherwise match. This enforces the
   * server's default policy that outgoing connections use TLS.
   *
   * @param prefixes the configured allowlist entries
   * @param url the candidate URL
   * @param allowInsecure if false, plain-{@code http} candidates are rejected before any
   *     prefix-matching is attempted
   * @return true if the candidate matches at least one prefix per the rules above
   */
  public static boolean matches(
      @Nonnull final List<String> prefixes,
      @Nonnull final String url,
      final boolean allowInsecure) {
    if (!allowInsecure && isPlainHttp(url)) {
      return false;
    }
    return prefixes.stream().anyMatch(prefix -> matches(prefix, url));
  }

  /**
   * Returns true if the candidate URL is allowed by the given prefix.
   *
   * <p>This single-prefix variant performs only structural matching and does not enforce the
   * insecure-URL policy: callers that need that policy must use the list-based variant or apply the
   * check separately.
   *
   * @param prefix the configured allowlist entry
   * @param url the candidate URL
   * @return true if the candidate matches the prefix per the rules above
   */
  public static boolean matches(@Nonnull final String prefix, @Nonnull final String url) {
    final String bareScheme = bareSchemeOf(prefix);
    if (bareScheme != null) {
      // A bare-scheme prefix (e.g. "file://" or "s3://") admits any URL on that scheme,
      // except for http(s): a bare HTTP scheme would trust every host on the network and is
      // the very mis-configuration this allowlist exists to prevent.
      if (isHttp(bareScheme)) {
        return false;
      }
      return url.toLowerCase(Locale.ROOT).startsWith(bareScheme + "://");
    }
    final Matcher prefixParts = URL_PARTS.matcher(prefix);
    if (!prefixParts.matches()) {
      return false;
    }
    if (isHttp(prefixParts.group(1))) {
      final URI prefixUri = parse(prefix);
      return prefixUri != null && matchesHttp(prefixUri, url);
    }
    return matchesNonHttp(prefixParts, url);
  }

  private static boolean matchesNonHttp(
      @Nonnull final Matcher prefixParts, @Nonnull final String url) {
    final Matcher candidateParts = URL_PARTS.matcher(url);
    if (!candidateParts.matches()) {
      return false;
    }
    final String candidatePath = candidateParts.group(3);
    return prefixParts.group(1).equalsIgnoreCase(candidateParts.group(1))
        && authorityOf(prefixParts).equals(authorityOf(candidateParts))
        && !canTraverse(candidatePath)
        && pathContains(prefixParts.group(3), candidatePath);
  }

  @Nonnull
  private static String authorityOf(@Nonnull final Matcher urlParts) {
    return Objects.requireNonNullElse(urlParts.group(2), "");
  }

  private static boolean canTraverse(@Nonnull final String path) {
    return path.chars().anyMatch(c -> GLOB_TRAVERSAL_CHARACTERS.indexOf(c) >= 0)
        || Arrays.asList(path.split("/")).contains("..");
  }

  private static boolean isPlainHttp(@Nonnull final String url) {
    final URI uri = parse(url);
    if (uri == null || uri.getScheme() == null) {
      return false;
    }
    return "http".equals(uri.getScheme().toLowerCase(Locale.ROOT));
  }

  private static String bareSchemeOf(@Nonnull final String prefix) {
    final int delim = prefix.indexOf("://");
    if (delim <= 0 || delim != prefix.length() - 3) {
      return null;
    }
    final String scheme = prefix.substring(0, delim);
    if (scheme.isEmpty()) {
      return null;
    }
    return scheme.toLowerCase(Locale.ROOT);
  }

  private static boolean matchesHttp(@Nonnull final URI prefixUri, @Nonnull final String url) {
    final URI candidate = parse(url);
    if (candidate == null) {
      return false;
    }
    if (candidate.getRawUserInfo() != null) {
      return false;
    }
    final String prefixScheme = prefixUri.getScheme().toLowerCase(Locale.ROOT);
    final String candidateScheme =
        candidate.getScheme() == null ? null : candidate.getScheme().toLowerCase(Locale.ROOT);
    if (!prefixScheme.equals(candidateScheme)) {
      return false;
    }
    final String prefixHost = lowerHost(prefixUri);
    final String candidateHost = lowerHost(candidate);
    if (prefixHost == null || !prefixHost.equals(candidateHost)) {
      return false;
    }
    if (effectivePort(prefixUri) != effectivePort(candidate)) {
      return false;
    }
    return pathContains(prefixUri.getPath(), candidate.getPath());
  }

  private static boolean isHttp(@Nonnull final String scheme) {
    final String lower = scheme.toLowerCase(Locale.ROOT);
    return "http".equals(lower) || "https".equals(lower);
  }

  private static String lowerHost(@Nonnull final URI uri) {
    final String host = uri.getHost();
    return host == null ? null : host.toLowerCase(Locale.ROOT);
  }

  private static int effectivePort(@Nonnull final URI uri) {
    final int explicit = uri.getPort();
    if (explicit != -1) {
      return explicit;
    }
    final String scheme = uri.getScheme();
    if (scheme == null) {
      return -1;
    }
    return switch (scheme.toLowerCase(Locale.ROOT)) {
      case "http" -> 80;
      case "https" -> 443;
      default -> -1;
    };
  }

  private static boolean pathContains(final String prefixPath, final String candidatePath) {
    final String normalisedPrefix = normalisePath(prefixPath);
    final String normalisedCandidate = normalisePath(candidatePath);
    if (normalisedPrefix.isEmpty()) {
      return true;
    }
    if (normalisedCandidate.equals(normalisedPrefix)) {
      return true;
    }
    return normalisedCandidate.startsWith(normalisedPrefix + "/");
  }

  private static String normalisePath(final String path) {
    if (path == null || path.isEmpty() || "/".equals(path)) {
      return "";
    }
    return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
  }

  private static URI parse(@Nonnull final String value) {
    try {
      return new URI(value);
    } catch (final URISyntaxException e) {
      return null;
    }
  }
}
