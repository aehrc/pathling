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

package au.csiro.pathling.terminology.store;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

import com.github.tomakehurst.wiremock.WireMockServer;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Stubs a FHIR package registry on a {@link WireMockServer}, so the registry checksum lookup can be
 * exercised without contacting a real registry. A registry answers {@code GET /{name}} with a
 * listing of every published version, each of which may carry {@code dist.shasum}.
 *
 * @author John Grimes
 */
public final class RegistryStub {

  /**
   * The path prefix that {@link #stubRedirect} serves, redirecting to the real listing. A test
   * points the verifier at {@code <baseUrl>/redirect} to exercise redirect following.
   */
  public static final String REDIRECT_PREFIX = "/redirect";

  private RegistryStub() {
    // Test helper.
  }

  /**
   * Returns the path a lookup of the named package requests, with the name percent-encoded.
   *
   * @param packageName the package name
   * @return the request path, including its leading slash
   */
  @Nonnull
  public static String path(@Nonnull final String packageName) {
    return "/" + URLEncoder.encode(packageName, StandardCharsets.UTF_8).replace("+", "%20");
  }

  /**
   * Stubs the listing of a package with a single published version.
   *
   * @param server the server to stub on
   * @param packageName the package name
   * @param version the published version
   * @param shasum the SHA-1 published for the version, or null to omit {@code dist.shasum}
   */
  public static void stubListing(
      @Nonnull final WireMockServer server,
      @Nonnull final String packageName,
      @Nonnull final String version,
      @Nullable final String shasum) {
    final Map<String, String> versions = new LinkedHashMap<>();
    versions.put(version, shasum);
    stubListing(server, packageName, versions);
  }

  /**
   * Stubs the listing of a package with several published versions.
   *
   * @param server the server to stub on
   * @param packageName the package name
   * @param shasumsByVersion the SHA-1 published for each version, a null value omitting {@code
   *     dist.shasum} for that version
   */
  public static void stubListing(
      @Nonnull final WireMockServer server,
      @Nonnull final String packageName,
      @Nonnull final Map<String, String> shasumsByVersion) {
    stubBody(server, packageName, listingJson(packageName, shasumsByVersion));
  }

  /**
   * Stubs the listing of a package whose version carries no {@code dist} object at all.
   *
   * @param server the server to stub on
   * @param packageName the package name
   * @param version the published version
   */
  public static void stubListingWithoutDist(
      @Nonnull final WireMockServer server,
      @Nonnull final String packageName,
      @Nonnull final String version) {
    final String body =
        "{\"name\":\""
            + escape(packageName)
            + "\",\"versions\":{\""
            + escape(version)
            + "\":{\"name\":\""
            + escape(packageName)
            + "\",\"version\":\""
            + escape(version)
            + "\"}}}";
    stubBody(server, packageName, body);
  }

  /**
   * Stubs a registry that does not know the package.
   *
   * @param server the server to stub on
   * @param packageName the package name
   */
  public static void stubNotFound(
      @Nonnull final WireMockServer server, @Nonnull final String packageName) {
    server.stubFor(
        get(urlEqualTo(path(packageName)))
            .willReturn(aResponse().withStatus(404).withBody("Not found")));
  }

  /**
   * Stubs a registry that fails with a server error.
   *
   * @param server the server to stub on
   * @param packageName the package name
   */
  public static void stubServerError(
      @Nonnull final WireMockServer server, @Nonnull final String packageName) {
    server.stubFor(
        get(urlEqualTo(path(packageName)))
            .willReturn(aResponse().withStatus(500).withBody("Internal server error")));
  }

  /**
   * Stubs a registry that answers with something that is not a version listing.
   *
   * @param server the server to stub on
   * @param packageName the package name
   */
  public static void stubMalformed(
      @Nonnull final WireMockServer server, @Nonnull final String packageName) {
    stubBody(server, packageName, "this is not JSON {");
  }

  /**
   * Stubs a redirect from {@link #REDIRECT_PREFIX} to the real listing of a package, which is
   * stubbed as well.
   *
   * @param server the server to stub on
   * @param packageName the package name
   * @param version the published version
   * @param shasum the SHA-1 published for the version, or null to omit {@code dist.shasum}
   */
  public static void stubRedirect(
      @Nonnull final WireMockServer server,
      @Nonnull final String packageName,
      @Nonnull final String version,
      @Nullable final String shasum) {
    stubListing(server, packageName, version, shasum);
    server.stubFor(
        get(urlEqualTo(REDIRECT_PREFIX + path(packageName)))
            .willReturn(
                aResponse()
                    .withStatus(302)
                    .withHeader("Location", server.baseUrl() + path(packageName))));
  }

  /**
   * Renders the version listing document a registry publishes for a package.
   *
   * @param packageName the package name
   * @param shasumsByVersion the SHA-1 published for each version, a null value omitting {@code
   *     dist.shasum} for that version
   * @return the listing document as JSON
   */
  @Nonnull
  public static String listingJson(
      @Nonnull final String packageName, @Nonnull final Map<String, String> shasumsByVersion) {
    final StringBuilder builder = new StringBuilder();
    builder
        .append("{\"_id\":\"")
        .append(escape(packageName))
        .append("\",\"name\":\"")
        .append(escape(packageName))
        .append("\",\"versions\":{");
    boolean first = true;
    for (final Map.Entry<String, String> entry : shasumsByVersion.entrySet()) {
      if (!first) {
        builder.append(",");
      }
      first = false;
      final String version = entry.getKey();
      builder
          .append("\"")
          .append(escape(version))
          .append("\":{\"name\":\"")
          .append(escape(packageName))
          .append("\",\"version\":\"")
          .append(escape(version))
          .append("\",\"dist\":{\"tarball\":\"https://example.com/")
          .append(escape(packageName))
          .append("/")
          .append(escape(version))
          .append("\"");
      if (entry.getValue() != null) {
        builder.append(",\"shasum\":\"").append(escape(entry.getValue())).append("\"");
      }
      builder.append("}}");
    }
    builder.append("}}");
    return builder.toString();
  }

  private static void stubBody(
      @Nonnull final WireMockServer server,
      @Nonnull final String packageName,
      @Nonnull final String body) {
    server.stubFor(
        get(urlEqualTo(path(packageName)))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(body)));
  }

  @Nonnull
  private static String escape(@Nonnull final String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
