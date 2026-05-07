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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link UrlAllowlist}.
 *
 * @author John Grimes
 */
class UrlAllowlistTest {

  // Subdomain confusion. A bare-host prefix (no trailing slash) must not match a candidate
  // whose host extends the prefix at a non-DNS boundary, e.g. "example.com.evil.com".
  @Test
  void rejectsSubdomainConfusion() {
    final String prefix = "https://allowed.example.com/";
    assertThat(UrlAllowlist.matches(prefix, "https://allowed.example.com.evil.com/foo")).isFalse();
    assertThat(UrlAllowlist.matches(prefix, "https://allowed.example.comevil.com/foo")).isFalse();
  }

  // Userinfo bypass. A candidate whose authority contains userinfo (the part before '@') is
  // rejected outright, since the effective host is whatever follows '@', not what precedes it.
  @Test
  void rejectsUserinfoBypass() {
    final String prefix = "https://allowed.example.com/";
    assertThat(UrlAllowlist.matches(prefix, "https://allowed.example.com@evil.com/foo")).isFalse();
    assertThat(UrlAllowlist.matches(prefix, "https://user:pass@allowed.example.com/foo")).isFalse();
  }

  // Host comparison is case-insensitive per RFC 3986 section 3.2.2.
  @Test
  void matchesHostCaseInsensitively() {
    assertThat(UrlAllowlist.matches("https://Allowed.Example.com/", "https://allowed.example.com/"))
        .isTrue();
    assertThat(UrlAllowlist.matches("https://allowed.example.com/", "https://ALLOWED.example.COM/"))
        .isTrue();
  }

  // Scheme comparison is case-insensitive per RFC 3986 section 3.1.
  @Test
  void matchesSchemeCaseInsensitively() {
    assertThat(UrlAllowlist.matches("HTTPS://example.com/", "https://example.com/")).isTrue();
  }

  // Different schemes never match, even if the hosts are identical.
  @Test
  void rejectsSchemeMismatch() {
    assertThat(UrlAllowlist.matches("https://example.com/", "http://example.com/")).isFalse();
  }

  // Default ports are inferred from the scheme: 80 for http, 443 for https.
  @Test
  void treatsDefaultPortsAsEquivalent() {
    assertThat(UrlAllowlist.matches("https://example.com/", "https://example.com:443/")).isTrue();
    assertThat(UrlAllowlist.matches("https://example.com:443/", "https://example.com/")).isTrue();
    assertThat(UrlAllowlist.matches("http://example.com/", "http://example.com:80/")).isTrue();
  }

  // Explicit port mismatches are rejected.
  @Test
  void rejectsPortMismatch() {
    assertThat(UrlAllowlist.matches("https://example.com:8443/", "https://example.com/")).isFalse();
    assertThat(UrlAllowlist.matches("https://example.com/", "https://example.com:8443/")).isFalse();
  }

  // The candidate path must equal the prefix path or extend it at a path-segment boundary.
  // "/foo" must not match "/foobar".
  @Test
  void requiresPathSegmentBoundary() {
    assertThat(UrlAllowlist.matches("https://example.com/foo", "https://example.com/foo")).isTrue();
    assertThat(UrlAllowlist.matches("https://example.com/foo", "https://example.com/foo/bar"))
        .isTrue();
    assertThat(UrlAllowlist.matches("https://example.com/foo", "https://example.com/foobar"))
        .isFalse();
  }

  // An empty prefix path and "/" are equivalent and match any candidate path on the same host.
  @Test
  void treatsEmptyAndSlashPrefixPathsAsEquivalent() {
    assertThat(UrlAllowlist.matches("https://example.com", "https://example.com/anything"))
        .isTrue();
    assertThat(UrlAllowlist.matches("https://example.com/", "https://example.com/anything"))
        .isTrue();
    assertThat(UrlAllowlist.matches("https://example.com", "https://example.com")).isTrue();
  }

  // Trailing slashes on the prefix path are ignored; the boundary check works either way.
  @Test
  void normalisesTrailingSlashOnPrefix() {
    assertThat(UrlAllowlist.matches("https://example.com/foo/", "https://example.com/foo"))
        .isTrue();
    assertThat(UrlAllowlist.matches("https://example.com/foo/", "https://example.com/foo/bar"))
        .isTrue();
    assertThat(UrlAllowlist.matches("https://example.com/foo/", "https://example.com/foobar"))
        .isFalse();
  }

  // Non-HTTP schemes (e.g. s3://) fall back to case-sensitive string prefix matching.
  @Test
  void usesStringPrefixForNonHttpSchemes() {
    assertThat(UrlAllowlist.matches("s3://my-bucket/", "s3://my-bucket/file.ndjson")).isTrue();
    assertThat(UrlAllowlist.matches("s3://my-bucket/", "s3://other-bucket/file.ndjson")).isFalse();
    assertThat(UrlAllowlist.matches("file:///staging/", "file:///staging/data.ndjson")).isTrue();
  }

  // A bare-scheme prefix on a non-HTTP scheme (e.g. "file://" or "s3://") admits any URL on
  // that scheme. This preserves operator ergonomics for staging filesystems where every
  // candidate path is a file:// URL.
  @Test
  void bareNonHttpSchemeAdmitsAnyUrlOnThatScheme() {
    assertThat(UrlAllowlist.matches("file://", "file:///staging/data.ndjson")).isTrue();
    assertThat(UrlAllowlist.matches("s3://", "s3://any-bucket/file.ndjson")).isTrue();
    assertThat(UrlAllowlist.matches("file://", "https://example.com/")).isFalse();
  }

  // A bare HTTP(S) scheme is rejected as misconfigured: it would trust every host on the
  // network, which is the bypass this allowlist exists to prevent. Operators must specify a
  // host.
  @Test
  void bareHttpSchemeIsAlwaysRejected() {
    assertThat(UrlAllowlist.matches("https://", "https://example.com/")).isFalse();
    assertThat(UrlAllowlist.matches("http://", "http://example.com/")).isFalse();
  }

  // Malformed candidates and prefixes are rejected rather than throwing.
  @Test
  void rejectsMalformedInputs() {
    assertThat(UrlAllowlist.matches("https://example.com/", "not a url")).isFalse();
    assertThat(UrlAllowlist.matches("not a url", "https://example.com/")).isFalse();
    assertThat(UrlAllowlist.matches("", "https://example.com/")).isFalse();
  }

  // The list overload returns true if any prefix matches.
  @Test
  void listOverloadReturnsTrueIfAnyMatches() {
    final List<String> prefixes =
        List.of("https://allowed1.example.com/", "https://allowed2.example.com/");
    assertThat(UrlAllowlist.matches(prefixes, "https://allowed2.example.com/foo")).isTrue();
    assertThat(UrlAllowlist.matches(prefixes, "https://disallowed.example.com/foo")).isFalse();
  }

  // Empty list never matches, regardless of the candidate.
  @Test
  void emptyListRejectsAll() {
    assertThat(UrlAllowlist.matches(List.of(), "https://example.com/")).isFalse();
  }
}
