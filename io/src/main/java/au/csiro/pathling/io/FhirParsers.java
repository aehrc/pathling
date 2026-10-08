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

import au.csiro.pathling.definition.DefinitionContext;
import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * The FHIR parsers that the routes without a Spark-native path use, which are bundles and XML in
 * either direction, configured so that what they parse and then write keeps what the source said
 * (decisions 83 and 84).
 *
 * <p>A parser is made where it is used, inside the function Spark runs, because neither a parser
 * nor the FHIR context behind it can be serialised. What is serialised instead is what the context
 * is built from: the FHIR version and the classes of the custom resource types that the definitions
 * describe, such as {@code ViewDefinition}. The context is built once per executor for each such
 * combination, so a parser can read and write whatever resource type the definitions describe.
 */
final class FhirParsers implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * The contexts built so far on this JVM, by the version and custom types they were built from.
   */
  @Nonnull private static final Map<List<Object>, FhirContext> CONTEXTS = new ConcurrentHashMap<>();

  @Nonnull private final FhirVersionEnum version;

  @Nonnull private final ArrayList<Class<? extends IBaseResource>> customTypes;

  private FhirParsers(
      @Nonnull final FhirVersionEnum version,
      @Nonnull final ArrayList<Class<? extends IBaseResource>> customTypes) {
    this.version = version;
    this.customTypes = customTypes;
  }

  /**
   * Returns parsers of the standard resource types of FHIR R4 only.
   *
   * @return the parsers
   */
  @Nonnull
  static FhirParsers standard() {
    return new FhirParsers(FhirVersionEnum.R4, new ArrayList<>());
  }

  /**
   * Returns parsers of the resource types the definitions describe. Definitions that are not backed
   * by a FHIR context describe the standard types of FHIR R4.
   *
   * @param definitions the definitions that say which resource types exist
   * @return the parsers
   */
  @Nonnull
  static FhirParsers of(@Nonnull final DefinitionContext definitions) {
    if (!(definitions instanceof final FhirDefinitionContext fhirDefinitions)) {
      return standard();
    }
    final FhirContext context = fhirDefinitions.getFhirContext();
    final FhirVersionEnum version = context.getVersion().getVersion();
    final FhirContext standard = FhirContext.forCached(version);
    final ArrayList<Class<? extends IBaseResource>> customTypes =
        registeredDefinitions(context).stream()
            .filter(definition -> !standard.getResourceTypes().contains(definition.getName()))
            .sorted(Comparator.comparing(RuntimeResourceDefinition::getName))
            .map(RuntimeResourceDefinition::getImplementingClass)
            .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
    return new FhirParsers(version, customTypes);
  }

  /**
   * Returns the definitions of every resource type that a context has registered, custom ones
   * included.
   *
   * <p>HAPI has no public way to list them: {@code getResourceTypes} omits the types registered
   * with {@code registerCustomType}. The package-private method that lists them is called through a
   * method handle until the layout no longer depends on HAPI for definitions, and {@code
   * CustomResourceTypeTest} fails if a HAPI upgrade removes it.
   */
  @Nonnull
  @SuppressWarnings("unchecked")
  private static Collection<RuntimeResourceDefinition> registeredDefinitions(
      @Nonnull final FhirContext context) {
    try {
      return (Collection<RuntimeResourceDefinition>)
          MethodHandles.privateLookupIn(FhirContext.class, MethodHandles.lookup())
              .findVirtual(
                  FhirContext.class,
                  "getAllResourceDefinitions",
                  MethodType.methodType(Collection.class))
              .invoke(context);
    } catch (final Throwable e) {
      throw new IllegalStateException(
          "Cannot list the resource types registered with the FHIR context", e);
    }
  }

  /**
   * Returns a parser of FHIR JSON, which is also the parser a resource parsed from a bundle or from
   * XML is written out with.
   *
   * @return the parser
   */
  @Nonnull
  IParser json() {
    return configured(context().newJsonParser());
  }

  /**
   * Returns a parser of FHIR XML, which is also the parser a resource is written out as XML with.
   *
   * @return the parser
   */
  @Nonnull
  IParser xml() {
    return configured(context().newXmlParser());
  }

  @Nonnull
  private FhirContext context() {
    if (customTypes.isEmpty()) {
      return FhirContext.forCached(version);
    }
    return CONTEXTS.computeIfAbsent(
        List.of(version, customTypes),
        key -> {
          final FhirContext context = new FhirContext(version);
          customTypes.forEach(context::registerCustomType);
          return context;
        });
  }

  @Nonnull
  private static IParser configured(@Nonnull final IParser parser) {
    // A resource in a bundle keeps the identifier it carries, rather than taking one from the full
    // URL of its entry, as the previous encoder kept it.
    parser.setOverrideResourceIdWithBundleEntryFullUrl(false);
    // HAPI drops the version from a reference when it writes one by default, which would change a
    // versioned reference the source carries.
    parser.setStripVersionsFromReferences(false);
    parser.setPrettyPrint(false);
    return parser;
  }
}
