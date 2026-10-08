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

import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Base;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Resource;

/**
 * Explodes FHIR bundles into the resources they carry, one resource type at a time, resolving the
 * references between their entries (FR-007, FR-045).
 *
 * <p>A bundle is a transport carrier and never a resource type. The readers refuse to be asked for
 * {@code Bundle} before they get here (FR-007), and a bundle carried as the resource of an entry is
 * neither returned nor exploded in turn, as the previous encoder did not explode it. An entry that
 * carries no resource, such as a request to delete one, contributes nothing.
 *
 * <p>The output is FHIR JSON, one resource per row, all of the type asked for, which {@link
 * FhirJsonReader} then reads, whichever format the bundles were written in. Filtering to the one
 * type happens here, before the reader infers a schema, because the reader's input carries
 * resources of one type (decision 70). This is how the bundle routes of both readers work today,
 * and none of it is public (decision 84).
 *
 * <p>References are resolved as the previous encoder resolved them (decision 83). A reference that
 * is a URN naming the full URL of another entry is rewritten to that entry's identifier, which HAPI
 * reports as a relative reference carrying the version its metadata names, if any. Any other
 * reference, including a URN that names no entry or an entry without an identifier, is kept as
 * written. Only a {@code Reference} is resolved, wherever one occurs, including within an extension
 * and within the extension of a primitive; an element of another type that happens to be named
 * {@code reference} is left alone.
 *
 * <p>Each bundle is parsed with HAPI inside the function Spark runs, which plan.md records as a
 * deliberate exception to FR-050: there is no Spark-native path that preserves FHIR semantics for
 * this format. HAPI's parser ignores content the definitions do not describe, so such content in a
 * bundle is not reported as a finding the way it is for newline-delimited JSON (decision 83).
 */
final class BundleTransformer {

  /** The prefix of a reference that may name another entry of the same bundle. */
  @Nonnull private static final String URN = "urn:";

  /** The prefix of a reference to a contained resource. */
  @Nonnull private static final String CONTAINED = "#";

  private final boolean xml;

  @Nonnull private final FhirParsers parsers;

  private BundleTransformer(final boolean xml, @Nonnull final FhirParsers parsers) {
    this.xml = xml;
    this.parsers = parsers;
  }

  /**
   * Returns a transformer of bundles written as FHIR JSON.
   *
   * @return the transformer
   */
  @Nonnull
  static BundleTransformer json() {
    return json(FhirParsers.standard());
  }

  /**
   * Returns a transformer of bundles written as FHIR JSON, which reads the resource types the
   * parsers read.
   *
   * @param parsers the parsers to read the bundles with
   * @return the transformer
   */
  @Nonnull
  static BundleTransformer json(@Nonnull final FhirParsers parsers) {
    return new BundleTransformer(false, parsers);
  }

  /**
   * Returns a transformer of bundles written as FHIR XML. The resources it returns are FHIR JSON
   * all the same.
   *
   * @return the transformer
   */
  @Nonnull
  static BundleTransformer xml() {
    return xml(FhirParsers.standard());
  }

  /**
   * Returns a transformer of bundles written as FHIR XML, which reads the resource types the
   * parsers read.
   *
   * @param parsers the parsers to read the bundles with
   * @return the transformer
   */
  @Nonnull
  static BundleTransformer xml(@Nonnull final FhirParsers parsers) {
    return new BundleTransformer(true, parsers);
  }

  /**
   * Returns the resources of one type carried by bundles, as FHIR JSON with the references between
   * entries resolved. A document that is not a bundle fails the job that evaluates the result.
   *
   * @param resourceType the type of the resources to return, which the caller has already checked
   *     may be stored
   * @param bundles the bundles, one per row, where a null row is left out
   * @return the resources, one per row
   */
  @Nonnull
  Dataset<String> resources(
      @Nonnull final String resourceType, @Nonnull final Dataset<String> bundles) {
    // The function captures only these values, so it serialises without the transformer.
    final boolean fromXml = xml;
    final FhirParsers fhirParsers = parsers;
    return bundles.flatMap(
        (FlatMapFunction<String, String>)
            bundle -> explode(bundle, resourceType, fromXml, fhirParsers),
        Encoders.STRING());
  }

  @Nonnull
  private static Iterator<String> explode(
      @Nullable final String text,
      @Nonnull final String resourceType,
      final boolean fromXml,
      @Nonnull final FhirParsers parsers) {
    if (text == null) {
      return Collections.emptyIterator();
    }
    final IBaseResource parsed = (fromXml ? parsers.xml() : parsers.json()).parseResource(text);
    if (!(parsed instanceof final Bundle bundle)) {
      throw new IllegalArgumentException(
          "Expected a bundle and found a resource of type " + parsed.fhirType());
    }
    final IParser json = parsers.json();
    final List<Resource> resources =
        bundle.getEntry().stream()
            .map(BundleEntryComponent::getResource)
            .filter(Objects::nonNull)
            .filter(resource -> resourceType.equals(resource.fhirType()))
            .toList();
    resources.forEach(BundleTransformer::resolveReferences);
    return resources.stream().map(json::encodeResourceToString).iterator();
  }

  /**
   * Resolves the references within an element and everything beneath it, in place.
   *
   * <p>HAPI links a reference to the entry whose full URL it names when it parses a bundle, and
   * that link is what the resolution reads. The link is then removed, because HAPI writes a linked
   * resource that has no identifier into the referring resource as a contained resource, which
   * would add content the source did not carry. A link to a contained resource is kept, since that
   * is how HAPI keeps a contained resource it writes.
   */
  private static void resolveReferences(@Nonnull final Base element) {
    if (element instanceof final Reference reference) {
      resolve(reference);
    }
    element.children().stream()
        .flatMap(child -> child.getValues().stream())
        .filter(Objects::nonNull)
        .forEach(BundleTransformer::resolveReferences);
  }

  private static void resolve(@Nonnull final Reference reference) {
    if (!(reference.getResource() instanceof final Resource target)) {
      return;
    }
    final String value = reference.getReference();
    if (value != null && value.startsWith(CONTAINED)) {
      return;
    }
    if (value != null && value.startsWith(URN) && target.getIdElement().hasValue()) {
      reference.setReference(target.getIdElement().getValue());
    }
    reference.setResource(null);
  }
}
