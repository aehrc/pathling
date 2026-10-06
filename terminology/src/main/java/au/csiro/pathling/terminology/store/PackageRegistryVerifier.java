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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serial;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

/**
 * Checks a FHIR NPM package against the checksum its registry publishes, by fetching the package's
 * version listing and comparing the tarball's SHA-1 with {@code versions.<version>.dist.shasum}.
 *
 * <p>Only a checksum that is present and differs fails the import: every other outcome, from an
 * unreachable registry to a version published without a checksum, leaves the package unverified
 * with a reason, since a registry that cannot answer is not evidence that the bytes are wrong.
 *
 * @author John Grimes
 */
@Slf4j
public class PackageRegistryVerifier {

  /** The registry consulted when the caller names none. */
  public static final String DEFAULT_REGISTRY_URL = "https://packages.fhir.org";

  /** The time allowed to establish a connection to the registry. */
  private static final int CONNECT_TIMEOUT_MS = 10_000;

  /** The time allowed between bytes of the registry's answer. */
  private static final int READ_TIMEOUT_MS = 30_000;

  private static final String FIELD_VERSIONS = "versions";

  private static final String FIELD_DIST = "dist";

  private static final String FIELD_SHASUM = "shasum";

  private static final JsonFactory FACTORY = new JsonFactory();

  @Nonnull private final String registryUrl;

  /**
   * Creates a verifier pointed at a registry.
   *
   * @param registryUrl the registry base URL, or null to use {@link #DEFAULT_REGISTRY_URL}; any
   *     trailing slashes are ignored
   */
  public PackageRegistryVerifier(@Nullable final String registryUrl) {
    this.registryUrl = normalise(registryUrl == null ? DEFAULT_REGISTRY_URL : registryUrl);
  }

  /**
   * Returns the registry this verifier consults, as it will be recorded in the store manifest.
   *
   * @return the normalised registry base URL
   */
  @Nonnull
  public String getRegistryUrl() {
    return registryUrl;
  }

  /**
   * Checks a package's bytes against the checksum the registry publishes for its version.
   *
   * @param packageName the package name from its {@code package.json}
   * @param packageVersion the package version from its {@code package.json}
   * @param sha1Hex the SHA-1 of the tarball, as lowercase hexadecimal
   * @return a verified result naming the registry, or an unverified result carrying the reason no
   *     comparison could be made
   * @throws TerminologyImportException if the registry publishes a checksum that differs
   */
  @Nonnull
  public PackageVerificationResult verify(
      @Nonnull final String packageName,
      @Nonnull final String packageVersion,
      @Nonnull final String sha1Hex) {
    final String url = registryUrl + "/" + encodeSegment(packageName);
    final String published;
    try {
      published = fetchShasum(url, packageVersion);
    } catch (final UnusableAnswer e) {
      return PackageVerificationResult.unverified(e.getMessage());
    } catch (final IOException e) {
      final String message = e.getMessage();
      return PackageVerificationResult.unverified(
          message == null ? e.getClass().getSimpleName() : message);
    }
    if (published == null) {
      return PackageVerificationResult.unverified("the registry publishes no checksum");
    }
    if (!published.equalsIgnoreCase(sha1Hex)) {
      throw new TerminologyImportException(
          "The package "
              + packageName
              + " "
              + packageVersion
              + " does not match the registry checksum published by "
              + registryUrl
              + ": expected SHA-1 "
              + published
              + " but the source has SHA-1 "
              + sha1Hex
              + ". The source may be corrupt or tampered with; import it anyway by turning off the"
              + " verifyPackage option.");
    }
    return PackageVerificationResult.verified(registryUrl);
  }

  /**
   * Fetches the checksum a registry publishes for a version of a package.
   *
   * @param url the listing URL
   * @param packageVersion the version whose checksum is wanted
   * @return the published checksum, or null if the version carries none
   * @throws UnusableAnswer if the registry did not answer with a listing holding the version
   * @throws IOException if the registry could not be reached
   */
  @Nullable
  private static String fetchShasum(@Nonnull final String url, @Nonnull final String packageVersion)
      throws IOException {
    final RequestConfig config =
        RequestConfig.custom()
            .setConnectTimeout(CONNECT_TIMEOUT_MS)
            .setConnectionRequestTimeout(CONNECT_TIMEOUT_MS)
            .setSocketTimeout(READ_TIMEOUT_MS)
            .setRedirectsEnabled(true)
            .build();
    try (CloseableHttpClient client =
        HttpClients.custom().setDefaultRequestConfig(config).build()) {
      final HttpGet request = new HttpGet(url);
      request.setHeader("Accept", "application/json");
      try (CloseableHttpResponse response = client.execute(request)) {
        final int status = response.getStatusLine().getStatusCode();
        if (status == 404) {
          throw new UnusableAnswer("package not found");
        }
        if (status < 200 || status >= 300) {
          throw new UnusableAnswer("HTTP " + status);
        }
        final HttpEntity entity = response.getEntity();
        if (entity == null) {
          throw new UnusableAnswer("unexpected response");
        }
        try (InputStream body = entity.getContent()) {
          return readShasum(body, packageVersion);
        }
      }
    }
  }

  /**
   * Walks a version listing to the checksum of one version, skipping everything else, so a listing
   * of hundreds of versions costs no more than the bytes it takes to reach the one that matters.
   *
   * @param body the listing document
   * @param packageVersion the version whose checksum is wanted
   * @return the published checksum, or null if the version carries none
   * @throws UnusableAnswer if the document is not a listing holding the version
   * @throws IOException if the body cannot be read
   */
  @Nullable
  private static String readShasum(
      @Nonnull final InputStream body, @Nonnull final String packageVersion) throws IOException {
    try (JsonParser parser = FACTORY.createParser(body)) {
      if (parser.nextToken() != JsonToken.START_OBJECT) {
        throw new UnusableAnswer("unexpected response");
      }
      while (parser.nextToken() == JsonToken.FIELD_NAME) {
        final boolean versions = FIELD_VERSIONS.equals(parser.currentName());
        parser.nextToken();
        if (!versions) {
          parser.skipChildren();
          continue;
        }
        if (parser.currentToken() != JsonToken.START_OBJECT) {
          throw new UnusableAnswer("unexpected response");
        }
        return readVersion(parser, packageVersion);
      }
    } catch (final JsonProcessingException e) {
      throw new UnusableAnswer("unexpected response");
    }
    throw new UnusableAnswer("unexpected response");
  }

  /** Reads the checksum of one version out of the {@code versions} object. */
  @Nullable
  private static String readVersion(
      @Nonnull final JsonParser parser, @Nonnull final String packageVersion) throws IOException {
    while (parser.nextToken() == JsonToken.FIELD_NAME) {
      final boolean wanted = packageVersion.equals(parser.currentName());
      parser.nextToken();
      if (!wanted) {
        parser.skipChildren();
        continue;
      }
      return readDist(parser);
    }
    throw new UnusableAnswer("version not listed");
  }

  /** Reads {@code dist.shasum} out of the metadata of one version. */
  @Nullable
  private static String readDist(@Nonnull final JsonParser parser) throws IOException {
    if (parser.currentToken() != JsonToken.START_OBJECT) {
      throw new UnusableAnswer("unexpected response");
    }
    String shasum = null;
    while (parser.nextToken() == JsonToken.FIELD_NAME) {
      final boolean dist = FIELD_DIST.equals(parser.currentName());
      parser.nextToken();
      if (!dist) {
        parser.skipChildren();
        continue;
      }
      if (parser.currentToken() != JsonToken.START_OBJECT) {
        parser.skipChildren();
        continue;
      }
      while (parser.nextToken() == JsonToken.FIELD_NAME) {
        final boolean isShasum = FIELD_SHASUM.equals(parser.currentName());
        parser.nextToken();
        if (isShasum) {
          shasum = parser.getValueAsString();
        } else {
          parser.skipChildren();
        }
      }
    }
    return shasum;
  }

  @Nonnull
  private static String encodeSegment(@Nonnull final String packageName) {
    // A path segment encodes a space as %20, where the form encoding of URLEncoder uses a plus.
    return URLEncoder.encode(packageName, StandardCharsets.UTF_8).replace("+", "%20");
  }

  @Nonnull
  private static String normalise(@Nonnull final String url) {
    int end = url.length();
    while (end > 0 && url.charAt(end - 1) == '/') {
      end--;
    }
    return url.substring(0, end);
  }

  /** A registry answer that carries no checksum to compare, along with the reason it does not. */
  private static final class UnusableAnswer extends IOException {

    @Serial private static final long serialVersionUID = 1L;

    UnusableAnswer(@Nonnull final String reason) {
      super(reason);
    }
  }
}
