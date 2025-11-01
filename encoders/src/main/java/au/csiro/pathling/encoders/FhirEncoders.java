/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2025 Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
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
 *
 */

package au.csiro.pathling.encoders;

import au.csiro.pathling.encoders.datatypes.DataTypeMappings;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * Spark Encoders for FHIR Resources. This object is thread safe.
 */
public class FhirEncoders {

  /**
   * The reasonable default set of open types to encode with extension values.
   */
  public static final Set<String> STANDARD_OPEN_TYPES = Set.of(
      "boolean",
      "code",
      "date",
      "dateTime",
      "decimal",
      "integer",
      "string",
      "Coding",
      "CodeableConcept",
      "Address",
      "Identifier",
      "Reference"
  );

  /**
   * All possible open types in FHIR R4.
   */
  public static final Set<String> ALL_OPEN_TYPES = Set.of("base64Binary", "boolean", "canonical",
      "code", "date", "dateTime", "decimal", "id", "instant", "integer", "markdown", "oid",
      "positiveInt", "string", "time", "unsignedInt", "uri", "url", "uuid", "Address", "Age",
      "Annotation", "Attachment", "CodeableConcept", "Coding", "ContactPoint", "Count", "Distance",
      "Duration", "HumanName", "Identifier", "Money", "Period", "Quantity", "Range", "Ratio",
      "Reference", "SampledData", "Signature", "Timing", "ContactDetail", "Contributor",
      "DataRequirement", "Expression", "ParameterDefinition", "RelatedArtifact",
      "TriggerDefinition", "UsageContext", "Dosage", "Meta");


  /**
   * Cache of Encoders instances.
   */
  private static final Map<FhirEncodersKey, FhirEncoders> ENCODERS = new ConcurrentHashMap<>();

  /**
   * Cache of mappings between Spark and FHIR types.
   */
  private static final Map<FhirVersionEnum, DataTypeMappings> DATA_TYPE_MAPPINGS = new ConcurrentHashMap<>();

  /**
   * Cache of FHIR contexts.
   */
  private static final Map<FhirVersionEnum, FhirContext> FHIR_CONTEXTS = new ConcurrentHashMap<>();

  /**
   * @return the FHIR context used by encoders produced by this instance.
   */
  @Getter
  private final FhirContext context;

  /**
   * The data type mappings used by the encoders instance.
   */
  private final DataTypeMappings mappings;

  /**
   * Cached encoders to avoid having to re-create them. Thread-safe using ConcurrentHashMap.
   */
  private final Map<Integer, ExpressionEncoder<?>> encoderCache = new ConcurrentHashMap<>();

  /**
   * The maximum nesting level for expansion of recursive data types.
   */
  private final int maxNestingLevel;

  /**
   * The list of types that are encoded within open types, such as extensions.
   */
  private final Set<String> openTypes;

  /**
   * Indicates whether FHIR extension support should be enabled.
   */
  private final boolean enableExtensions;

  /**
   * Consumers should generally use the {@link #forR4()} method, but this is made available for test
   * purposes and additional experimental mappings.
   *
   * @param context the FHIR context to use.
   * @param mappings mappings between Spark and FHIR data types.
   * @param maxNestingLevel maximum nesting level for expansion of recursive data types.
   * @param openTypes the list of types that are encoded within open types, such as extensions.
   * @param enableExtensions true if FHIR extension should be enabled.
   */
  public FhirEncoders(final FhirContext context, final DataTypeMappings mappings,
      final int maxNestingLevel, final Set<String> openTypes, final boolean enableExtensions) {
    this.context = context;
    this.mappings = mappings;
    this.maxNestingLevel = maxNestingLevel;
    this.openTypes = openTypes;
    this.enableExtensions = enableExtensions;
  }

  /**
   * Returns a cached FhirEncoders instance for the specified configuration, creating one if it
   * does not already exist. This method is thread-safe and ensures that encoders with identical
   * configurations are shared across the application.
   *
   * @param fhirVersion the FHIR version to use for encoding
   * @param maxNestingLevel the maximum nesting level for encoding complex resources
   * @param openTypes the set of open types to encode with extension values
   * @param enableExtensions whether to enable extension encoding
   * @return a FhirEncoders instance configured with the specified parameters
   */
  public static FhirEncoders getOrCreate(@Nonnull final FhirVersionEnum fhirVersion,
      final int maxNestingLevel,
      @Nonnull final Set<String> openTypes, final boolean enableExtensions) {
    final FhirEncodersKey key = new FhirEncodersKey(fhirVersion, maxNestingLevel, openTypes,
        enableExtensions);
    return ENCODERS.computeIfAbsent(key, k -> {
      final FhirContext context = contextFor(k.getFhirVersion());
      final DataTypeMappings mappings = mappingsFor(k.getFhirVersion());
      return new FhirEncoders(context, mappings, k.getMaxNestingLevel(), k.getOpenTypes(),
          k.isEnableExtensions());
    });
  }

  /**
   * Returns the FHIR context for the given version. This is effectively a cache so consuming code
   * does not need to recreate the context repeatedly.
   *
   * @param fhirVersion the version of FHIR to use
   * @return the FhirContext
   */
  public static FhirContext contextFor(final FhirVersionEnum fhirVersion) {
    return FHIR_CONTEXTS.computeIfAbsent(fhirVersion, v -> new FhirContext(fhirVersion));
  }

  /**
   * Returns the {@link DataTypeMappings} instance for the given FHIR version.
   *
   * @param fhirVersion the FHIR version for the data type mappings.
   * @return a DataTypeMappings instance.
   */
  static DataTypeMappings mappingsFor(final FhirVersionEnum fhirVersion) {
    return DATA_TYPE_MAPPINGS.computeIfAbsent(fhirVersion, version -> {
      final String dataTypesClassName;

      if (version == FhirVersionEnum.R4) {
        dataTypesClassName = "au.csiro.pathling.encoders.datatypes.R4DataTypeMappings";
      } else {
        throw new IllegalArgumentException("Unsupported FHIR version: " + version);
      }

      try {
        return (DataTypeMappings) Class.forName(dataTypesClassName).getDeclaredConstructor()
            .newInstance();
      } catch (final Exception createClassException) {
        throw new IllegalStateException("Unable to create the data mappings "
            + dataTypesClassName
            + ". This is typically because the HAPI FHIR dependencies for "
            + "the underlying data model are note present. Make sure the "
            + " hapi-fhir-structures-* and hapi-fhir-validation-resources-* "
            + " jars for the desired FHIR version are available on the class path.",
            createClassException);
      }
    });
  }

  /**
   * Returns a builder to create encoders for FHIR R4.
   *
   * @return a builder for encoders.
   */
  @Nonnull
  public static FhirEncoderBuilder forR4() {
    return forVersion(FhirVersionEnum.R4);
  }

  /**
   * Returns a builder to create encoders for the given FHIR version.
   *
   * @param fhirVersion the version of FHIR to use.
   * @return a builder for encoders.
   */
  @Nonnull
  public static FhirEncoderBuilder forVersion(final FhirVersionEnum fhirVersion) {
    return new FhirEncoderBuilder(fhirVersion);
  }

  /**
   * Returns an encoder for the given FHIR resource.
   *
   * @param resourceName the type of the resource to encode.
   * @param <T> the type of the resource to be encoded.
   * @return an encoder for the resource.
   */
  @SuppressWarnings("unchecked")
  public final <T extends IBaseResource> ExpressionEncoder<T> of(final String resourceName) {
    final RuntimeResourceDefinition definition = context.getResourceDefinition(resourceName);
    return of((Class<T>) definition.getImplementingClass());
  }

  /**
   * Returns an encoder for the given FHIR resource.
   *
   * @param type the type of the resource to encode.
   * @param <T> the type of the resource to be encoded.
   * @return an encoder for the resource.
   */
  @SuppressWarnings("unchecked")
  public final <T extends IBaseResource> ExpressionEncoder<T> of(final Class<T> type) {
    final RuntimeResourceDefinition definition = context.getResourceDefinition(type);

    final int key = type.getName().hashCode();
    return (ExpressionEncoder<T>) encoderCache.computeIfAbsent(key, k ->
        EncoderBuilder.of(definition,
            context,
            mappings,
            maxNestingLevel,
            scala.jdk.javaapi.CollectionConverters.asScala(openTypes).toSet(),
            enableExtensions));
  }

  /**
   * Returns the version of FHIR used by encoders produced by this instance.
   *
   * @return the version of FHIR used by encoders produced by this instance.
   */
  public FhirVersionEnum getFhirVersion() {
    return context.getVersion().getVersion();
  }

}
