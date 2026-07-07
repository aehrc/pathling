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

package au.csiro.pathling.terminology.local;

import au.csiro.pathling.ecl.EclToVclTranslator;
import au.csiro.pathling.vcl.Vcl;
import au.csiro.pathling.vcl.VclCodeValue;
import au.csiro.pathling.vcl.VclConjunction;
import au.csiro.pathling.vcl.VclDisjunction;
import au.csiro.pathling.vcl.VclExclusion;
import au.csiro.pathling.vcl.VclExpression;
import au.csiro.pathling.vcl.VclFilter;
import au.csiro.pathling.vcl.VclFilterOperator;
import au.csiro.pathling.vcl.VclRefsetMembership;
import au.csiro.pathling.vcl.VclSystemScoped;
import au.csiro.pathling.vcl.VclSystemUri;
import au.csiro.pathling.vcl.VclWildcard;
import jakarta.annotation.Nonnull;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Resolves a value set URL to the code system version it evaluates over and the {@link
 * VclExpression} that defines its members. It handles the SNOMED CT implicit value set forms
 * (all-concepts, reference set, is-a, and ECL) on both unversioned and edition/version-qualified
 * SNOMED URIs, and VCL implicit value set URLs. Content that is absent from the store resolves to
 * {@link Optional#empty()}, so the caller applies the unknown-content fallback.
 *
 * @author John Grimes
 */
public class ValueSetResolver {

  private static final String SNOMED_URI = "http://snomed.info/sct";
  private static final String VCL_URI_PREFIX = "http://fhir.org/VCL";
  private static final String CONCEPT = "concept";
  private static final Pattern SNOMED_VERSIONED =
      Pattern.compile("^http://snomed\\.info/x?sct/\\d+/version/\\d+$");

  @Nonnull private final List<CodeSystemEntry> catalogue;
  @Nonnull private final VersionResolver versionResolver;
  @jakarta.annotation.Nullable private final ValueSetStore valueSetStore;
  @jakarta.annotation.Nullable private final ComposeTranslator composeTranslator;

  /**
   * Creates a resolver over a catalogue of imported code system versions, without support for
   * explicit imported value sets.
   *
   * @param catalogue the code system versions in the store
   * @param versionResolver the resolver that selects a default version
   */
  public ValueSetResolver(
      @Nonnull final List<CodeSystemEntry> catalogue,
      @Nonnull final VersionResolver versionResolver) {
    this(catalogue, versionResolver, null);
  }

  /**
   * Creates a resolver that also resolves explicit imported FHIR value sets by canonical URL.
   *
   * @param catalogue the code system versions in the store
   * @param versionResolver the resolver that selects a default version
   * @param valueSetStore the catalogue of imported value sets, or null for none
   */
  public ValueSetResolver(
      @Nonnull final List<CodeSystemEntry> catalogue,
      @Nonnull final VersionResolver versionResolver,
      @jakarta.annotation.Nullable final ValueSetStore valueSetStore) {
    this.catalogue = catalogue;
    this.versionResolver = versionResolver;
    this.valueSetStore = valueSetStore;
    this.composeTranslator = valueSetStore == null ? null : new ComposeTranslator(valueSetStore);
  }

  /**
   * Resolves a value set URL.
   *
   * @param valueSetUrl the value set URL
   * @return the resolved value set, or empty if the referenced content is absent from the store
   * @throws au.csiro.pathling.terminology.local.AmbiguousVersionException if an unversioned SNOMED
   *     reference cannot select a single default edition
   * @throws au.csiro.pathling.ecl.UnsupportedEclConstructError if an ECL expression uses an
   *     unsupported construct
   * @throws au.csiro.pathling.ecl.EclParseException if an ECL expression is malformed
   * @throws au.csiro.pathling.vcl.VclParseException if a VCL expression is malformed
   */
  @Nonnull
  public Optional<ResolvedValueSet> resolve(@Nonnull final String valueSetUrl) {
    if (valueSetUrl.startsWith(VCL_URI_PREFIX)) {
      return resolveVcl(valueSetUrl);
    }
    if (valueSetUrl.startsWith(SNOMED_URI)) {
      return resolveSnomed(valueSetUrl);
    }
    return resolveExplicit(valueSetUrl);
  }

  /**
   * Resolves an explicit imported FHIR value set by canonical URL and optional {@code |version}.
   */
  @Nonnull
  private Optional<ResolvedValueSet> resolveExplicit(@Nonnull final String valueSetUrl) {
    if (valueSetStore == null || composeTranslator == null) {
      return Optional.empty();
    }
    final int pipe = valueSetUrl.indexOf('|');
    final String url = pipe < 0 ? valueSetUrl : valueSetUrl.substring(0, pipe);
    final String version = pipe < 0 ? null : valueSetUrl.substring(pipe + 1);
    return valueSetStore
        .resolve(url, version)
        .flatMap(composeTranslator::translate)
        .flatMap(
            compose ->
                resolveCodeSystemVersion(compose.getSystemUrl(), null)
                    .map(
                        id ->
                            new ResolvedValueSet(
                                id, compose.getSystemUrl(), compose.getExpression())));
  }

  @Nonnull
  private Optional<ResolvedValueSet> resolveSnomed(@Nonnull final String valueSetUrl) {
    final int query = valueSetUrl.indexOf('?');
    final String base = query < 0 ? valueSetUrl : valueSetUrl.substring(0, query);
    final String queryString = query < 0 ? "" : valueSetUrl.substring(query + 1);

    final String requestedVersion;
    if (SNOMED_URI.equals(base)) {
      requestedVersion = null;
    } else if (SNOMED_VERSIONED.matcher(base).matches()) {
      requestedVersion = base;
    } else {
      return Optional.empty();
    }

    final VclExpression expression = snomedExpression(queryString);
    if (expression == null) {
      return Optional.empty();
    }
    return resolveCodeSystemVersion(SNOMED_URI, requestedVersion)
        .map(id -> new ResolvedValueSet(id, SNOMED_URI, expression));
  }

  private VclExpression snomedExpression(@Nonnull final String queryString) {
    if (queryString.equals("fhir_vs") || queryString.equals("fhir_vs=")) {
      return new VclWildcard();
    }
    if (!queryString.startsWith("fhir_vs=")) {
      return null;
    }
    final String value = queryString.substring("fhir_vs=".length());
    if (value.isEmpty()) {
      return new VclWildcard();
    }
    if (value.startsWith("refset/")) {
      return new VclRefsetMembership(value.substring("refset/".length()));
    }
    if (value.startsWith("isa/")) {
      return new VclFilter(
          CONCEPT, VclFilterOperator.IS_A, new VclCodeValue(value.substring("isa/".length())));
    }
    if (value.startsWith("ecl/")) {
      final String ecl = decode(value.substring("ecl/".length()));
      return EclToVclTranslator.translate(ecl);
    }
    // Any other implicit form (e.g. an unsupported SNOMED value set) is unknown content.
    return null;
  }

  @Nonnull
  private Optional<ResolvedValueSet> resolveVcl(@Nonnull final String valueSetUrl) {
    final int query = valueSetUrl.indexOf('?');
    if (query < 0) {
      return Optional.empty();
    }
    final String queryString = valueSetUrl.substring(query + 1);
    if (!queryString.startsWith("v1=")) {
      return Optional.empty();
    }
    final VclExpression expression = Vcl.parse(decode(queryString.substring("v1=".length())));
    final Optional<VclSystemUri> primarySystem = primarySystem(expression);
    if (primarySystem.isEmpty()) {
      return Optional.empty();
    }
    final String systemUrl = primarySystem.get().getSystem();
    return resolveCodeSystemVersion(systemUrl, primarySystem.get().getVersion())
        .map(id -> new ResolvedValueSet(id, systemUrl, expression));
  }

  /**
   * Resolves a code system URL and optional version to the stable identifier of a stored code
   * system version. An explicit version selects exactly that version; an absent version selects the
   * default per the version-ordering rules. This backs both value set resolution and the per-coding
   * operations (lookup, subsumes, translate).
   *
   * @param url the code system canonical URL
   * @param requestedVersion the requested version, or null for the default
   * @return the stable system version identifier, or empty if the system or version is absent from
   *     the store
   * @throws au.csiro.pathling.terminology.local.AmbiguousVersionException if an unversioned SNOMED
   *     reference cannot select a single default edition
   */
  @Nonnull
  public Optional<String> resolveCodeSystemVersion(
      @Nonnull final String url, @jakarta.annotation.Nullable final String requestedVersion) {
    final List<CodeSystemEntry> candidates =
        catalogue.stream().filter(entry -> url.equals(entry.getUrl())).toList();
    if (candidates.isEmpty()) {
      return Optional.empty();
    }
    if (requestedVersion != null) {
      return candidates.stream()
          .filter(entry -> requestedVersion.equals(entry.getVersion()))
          .findFirst()
          .map(CodeSystemEntry::getSystemVersionId);
    }
    final CodeSystemEntry latest =
        versionResolver.getLatestOfVersions(candidates, CodeSystemEntry::getVersion, url);
    return Optional.ofNullable(latest).map(CodeSystemEntry::getSystemVersionId);
  }

  /** Finds the first system scope in an expression, which determines the code system it targets. */
  @Nonnull
  private static Optional<VclSystemUri> primarySystem(@Nonnull final VclExpression expression) {
    if (expression instanceof final VclSystemScoped scoped) {
      return Optional.of(scoped.getSystem());
    }
    if (expression instanceof final VclConjunction conjunction) {
      return firstSystem(conjunction.getOperands());
    }
    if (expression instanceof final VclDisjunction disjunction) {
      return firstSystem(disjunction.getOperands());
    }
    if (expression instanceof final VclExclusion exclusion) {
      return primarySystem(exclusion.getIncluded());
    }
    return Optional.empty();
  }

  @Nonnull
  private static Optional<VclSystemUri> firstSystem(@Nonnull final List<VclExpression> operands) {
    for (final VclExpression operand : operands) {
      final Optional<VclSystemUri> system = primarySystem(operand);
      if (system.isPresent()) {
        return system;
      }
    }
    return Optional.empty();
  }

  @Nonnull
  private static String decode(@Nonnull final String value) {
    return URLDecoder.decode(value, StandardCharsets.UTF_8);
  }
}
