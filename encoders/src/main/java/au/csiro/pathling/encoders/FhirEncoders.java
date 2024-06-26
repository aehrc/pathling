/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.encoders;

import au.csiro.pathling.encoders.datatypes.DataTypeMappings;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.Value;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.instance.model.api.IBaseResource;
import scala.collection.JavaConverters;

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
   * Cache of Encoders instances.
   */
  private static final Map<EncodersKey, FhirEncoders> ENCODERS = new HashMap<>();

  /**
   * Cache of mappings between Spark and FHIR types.
   */
  private static final Map<FhirVersionEnum, DataTypeMappings> DATA_TYPE_MAPPINGS = new HashMap<>();

  /**
   * Cache of FHIR contexts.
   */
  private static final Map<FhirVersionEnum, FhirContext> FHIR_CONTEXTS = new HashMap<>();

  /**
   * The FHIR context used by the encoders instance.
   */
  private final FhirContext context;

  /**
   * The data type mappings used by the encoders instance.
   */
  private final DataTypeMappings mappings;

  /**
   * Cached encoders to avoid having to re-create them.
   */
  private final Map<Integer, ExpressionEncoder<?>> encoderCache = new HashMap<>();

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
   * Returns the FHIR context for the given version. This is effectively a cache so consuming code
   * does not need to recreate the context repeatedly.
   *
   * @param fhirVersion the version of FHIR to use
   * @return the FhirContext
   */
  public static FhirContext contextFor(final FhirVersionEnum fhirVersion) {

    synchronized (FHIR_CONTEXTS) {

      FhirContext context = FHIR_CONTEXTS.get(fhirVersion);

      if (context == null) {

        context = new FhirContext(fhirVersion);

        FHIR_CONTEXTS.put(fhirVersion, context);
      }

      return context;
    }
  }

  /**
   * Returns the {@link DataTypeMappings} instance for the given FHIR version.
   *
   * @param fhirVersion the FHIR version for the data type mappings.
   * @return a DataTypeMappings instance.
   */
  static DataTypeMappings mappingsFor(final FhirVersionEnum fhirVersion) {

    synchronized (DATA_TYPE_MAPPINGS) {

      DataTypeMappings mappings = DATA_TYPE_MAPPINGS.get(fhirVersion);

      if (mappings == null) {
        final String dataTypesClassName;

        if (fhirVersion == FhirVersionEnum.R4) {
          dataTypesClassName = "au.csiro.pathling.encoders.datatypes.R4DataTypeMappings";
        } else {
          throw new IllegalArgumentException("Unsupported FHIR version: " + fhirVersion);
        }

        try {

          mappings = (DataTypeMappings) Class.forName(dataTypesClassName).getDeclaredConstructor()
              .newInstance();

          DATA_TYPE_MAPPINGS.put(fhirVersion, mappings);

        } catch (final Exception createClassException) {

          throw new IllegalStateException("Unable to create the data mappings "
              + dataTypesClassName
              + ". This is typically because the HAPI FHIR dependencies for "
              + "the underlying data model are note present. Make sure the "
              + " hapi-fhir-structures-* and hapi-fhir-validation-resources-* "
              + " jars for the desired FHIR version are available on the class path.",
              createClassException);
        }
      }

      return mappings;
    }
  }

  /**
   * Returns a builder to create encoders for FHIR R4.
   *
   * @return a builder for encoders.
   */
  public static Builder forR4() {

    return forVersion(FhirVersionEnum.R4);
  }

  /**
   * Returns a builder to create encoders for the given FHIR version.
   *
   * @param fhirVersion the version of FHIR to use.
   * @return a builder for encoders.
   */
  public static Builder forVersion(final FhirVersionEnum fhirVersion) {
    return new Builder(fhirVersion);
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

    final RuntimeResourceDefinition definition =
        context.getResourceDefinition(type);

    final int key = type.getName().hashCode();

    synchronized (encoderCache) {
      return (ExpressionEncoder<T>) encoderCache.computeIfAbsent(key, k ->
          EncoderBuilder.of(definition,
              context,
              mappings,
              maxNestingLevel,
              JavaConverters.asScalaSet(openTypes).toSet(),
              enableExtensions));
    }
  }

  /**
   * Returns the version of FHIR used by encoders produced by this instance.
   *
   * @return the version of FHIR used by encoders produced by this instance.
   */
  public FhirVersionEnum getFhirVersion() {

    return context.getVersion().getVersion();
  }

  /**
   * Returns the FHIR context used by encoders produced by this instance.
   *
   * @return the FHIR context used by encoders produced by this instance.
   */
  public FhirContext getContext() {
    return context;
  }

  /**
   * Immutable key to look up a matching encoders instance by configuration.
   */
  @Value
  private static class EncodersKey {

    FhirVersionEnum fhirVersion;
    int maxNestingLevel;
    Set<String> openTypes;
    boolean enableExtensions;
  }

  /**
   * Encoder builder. Specifies FHIR version and other parameters affecting encoder functionality,
   * such as max nesting level for recursive types with the fluent API.
   */
  public static class Builder {

    private static final boolean DEFAULT_ENABLE_EXTENSIONS = false;
    private static final int DEFAULT_MAX_NESTING_LEVEL = 0;

    private final FhirVersionEnum fhirVersion;
    private int maxNestingLevel;
    private Set<String> openTypes;
    private boolean enableExtensions;

    Builder(final FhirVersionEnum fhirVersion) {
      this.fhirVersion = fhirVersion;
      this.maxNestingLevel = DEFAULT_MAX_NESTING_LEVEL;
      this.openTypes = Collections.emptySet();
      this.enableExtensions = DEFAULT_ENABLE_EXTENSIONS;
    }

    /**
     * Set the maximum nesting level for recursive data types. Zero (0) indicates that all direct or
     * indirect fields of type T in element of type T should be skipped.
     *
     * @param maxNestingLevel the maximum nesting level
     * @return this builder
     */
    public Builder withMaxNestingLevel(final int maxNestingLevel) {
      this.maxNestingLevel = maxNestingLevel;
      return this;
    }

    /**
     * Sets the list of types that are encoded within open types, such as extensions.
     *
     * @param openTypes the list of types
     * @return this builder
     */
    public Builder withOpenTypes(final Set<String> openTypes) {
      this.openTypes = openTypes;
      return this;
    }

    /**
     * Sets the reasonable default list of types to be encoded for open types, such as extensions.
     *
     * @return this builder
     */
    public Builder withStandardOpenTypes() {
      return withOpenTypes(STANDARD_OPEN_TYPES);
    }

    /**
     * Sets the list of all types to be encoded for open types, such as extensions.
     *
     * @return this builder
     */
    public Builder withAllOpenTypes() {
      return withOpenTypes(
          Set.of("base64Binary", "boolean", "canonical", "code", "date", "dateTime", "decimal",
              "id", "instant", "integer", "markdown", "oid", "positiveInt", "string", "time",
              "unsignedInt", "uri", "url", "uuid", "Address", "Age", "Annotation", "Attachment",
              "CodeableConcept", "Coding", "ContactPoint", "Count", "Distance", "Duration",
              "HumanName", "Identifier", "Money", "Period", "Quantity", "Range", "Ratio",
              "Reference", "SampledData", "Signature", "Timing", "ContactDetail", "Contributor",
              "DataRequirement", "Expression", "ParameterDefinition", "RelatedArtifact",
              "TriggerDefinition", "UsageContext", "Dosage", "Meta"));
    }

    /**
     * Switches on/off the support for extensions in encoders.
     *
     * @param enable if extensions should be enabled.
     * @return this builder
     */
    public Builder withExtensionsEnabled(final boolean enable) {
      this.enableExtensions = enable;
      return this;
    }

    /**
     * Get or create an {@link FhirEncoders} instance that matches the builder's configuration.
     *
     * @return an Encoders instance.
     */
    public FhirEncoders getOrCreate() {

      final EncodersKey key = new EncodersKey(fhirVersion, maxNestingLevel,
          openTypes, enableExtensions);

      synchronized (ENCODERS) {

        FhirEncoders encoders = ENCODERS.get(key);

        // No instance with the given configuration found,
        // so create one.
        if (encoders == null) {

          final FhirContext context = contextFor(fhirVersion);
          final DataTypeMappings mappings = mappingsFor(fhirVersion);
          encoders = new FhirEncoders(context, mappings, maxNestingLevel, openTypes,
              enableExtensions);
          ENCODERS.put(key, encoders);
        }
        return encoders;
      }
    }
  }
}
