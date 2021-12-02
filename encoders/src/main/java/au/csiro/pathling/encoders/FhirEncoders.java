/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2021, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 */

package au.csiro.pathling.encoders;

import au.csiro.pathling.encoders.datatypes.DataTypeMappings;
import au.csiro.pathling.encoders1.EncoderBuilder1;
import au.csiro.pathling.encoders1.SchemaConverter1;
import au.csiro.pathling.encoders2.EncoderBuilder2;
import ca.uhn.fhir.context.BaseRuntimeElementCompositeDefinition;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import java.util.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.instance.model.api.IBaseResource;
import scala.collection.JavaConverters;

/**
 * Spark Encoders for FHIR Resources. This object is thread safe.
 */
public class FhirEncoders {

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
  private final Map<Integer, ExpressionEncoder> encoderCache = new HashMap<>();

  /**
   * The maximum nesting level for expansion of recursive data types.
   */
  private final int maxNestingLevel;

  /**
   * The encoder version to use.
   */
  private final int encoderVersion;

  /**
   * Consumers should generally use the {@link #forR4()} method, but this is made available for test
   * purposes and additional experimental mappings.
   *
   * @param context the FHIR context to use.
   * @param mappings mappings between Spark and FHIR data types.
   * @param maxNestingLevel maximum nesting level for expansion of recursive data types.
   * @param encoderVersion the encoder version to use.
   */
  public FhirEncoders(final FhirContext context, final DataTypeMappings mappings,
      final int maxNestingLevel, int encoderVersion) {

    assert (encoderVersion == 1 || encoderVersion == 2);
    this.context = context;
    this.mappings = mappings;
    this.maxNestingLevel = maxNestingLevel;
    this.encoderVersion = encoderVersion;
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
   * @param type the type of the resource to encode.
   * @param <T> the type of the resource to be encoded.
   * @return an encoder for the resource.
   */
  public final <T extends IBaseResource> ExpressionEncoder<T> of(final String type) {

    return of(type, new String[]{});
  }

  /**
   * Returns an encoder for the given FHIR resource.
   *
   * @param type the type of the resource to encode.
   * @param <T> the type of the resource to be encoded.
   * @return an encoder for the resource.
   */
  public final <T extends IBaseResource> ExpressionEncoder<T> of(final Class<T> type) {

    return of(type, Collections.emptyList());
  }

  /**
   * Returns an encoder for the given FHIR resource by name, as defined by the FHIR specification.
   *
   * @param resourceName the name of the FHIR resource to encode, such as "Encounter", "Condition",
   * "Observation", etc.
   * @param contained the names of FHIR resources contained to the encoded resource.
   * @param <T> the type of the resource to be encoded.
   * @return an encoder for the resource.
   */
  public <T extends IBaseResource> ExpressionEncoder<T> of(final String resourceName,
      final String... contained) {

    final RuntimeResourceDefinition definition = context.getResourceDefinition(resourceName);

    final List<Class<? extends IBaseResource>> containedClasses = new ArrayList<>();

    for (final String containedName : contained) {

      containedClasses.add(context.getResourceDefinition(containedName).getImplementingClass());
    }

    //noinspection unchecked
    return of((Class<T>) definition.getImplementingClass(), containedClasses);
  }

  /**
   * Returns an encoder for the given FHIR resource.
   *
   * @param type the type of the resource to encode.
   * @param contained a list of types for FHIR resources contained to the encoded resource.
   * @param <T> the type of the resource to be encoded.
   * @return an encoder for the resource.
   */
  public final <T extends IBaseResource> ExpressionEncoder<T> of(final Class<T> type,
      final Class<? extends IBaseResource>... contained) {

    final List<Class<? extends IBaseResource>> containedResourceList = new ArrayList<>();

    for (final Class element : contained) {

      if (IBaseResource.class.isAssignableFrom(element)) {

        //noinspection unchecked
        containedResourceList.add((Class<IBaseResource>) element);
      } else {

        throw new IllegalArgumentException("The contained classes provided must all implement  "
            + "FHIR IBaseResource");
      }
    }

    return of(type, containedResourceList);
  }

  /**
   * Returns an encoder for the given FHIR resource.
   *
   * @param type the type of the resource to encode.
   * @param contained a list of types for FHIR resources contained to the encoded resource.
   * @param <T> the type of the resource to be encoded.
   * @return an encoder for the resource.
   */
  public final <T extends IBaseResource> ExpressionEncoder<T> of(final Class<T> type,
      final List<Class<? extends IBaseResource>> contained) {

    final RuntimeResourceDefinition definition =
        context.getResourceDefinition(type);

    final List<BaseRuntimeElementCompositeDefinition<?>> containedDefinitions = new ArrayList<>();

    for (final Class<? extends IBaseResource> resource : contained) {

      containedDefinitions.add(context.getResourceDefinition(resource));
    }

    final StringBuilder keyBuilder = new StringBuilder(type.getName());

    for (final Class resource : contained) {

      keyBuilder.append(resource.getName());
    }

    final int key = keyBuilder.toString().hashCode();

    synchronized (encoderCache) {

      //noinspection unchecked
      ExpressionEncoder<T> encoder = encoderCache.get(key);

      if (encoder == null) {
        if (encoderVersion == 2) {
          //noinspection unchecked
          encoder = (ExpressionEncoder<T>)
              EncoderBuilder2.of(definition,
                  context,
                  mappings,
                  maxNestingLevel,
                  JavaConverters.asScalaBuffer(containedDefinitions));
        } else if (encoderVersion == 1) {
          //noinspection unchecked
          encoder = (ExpressionEncoder<T>)
              EncoderBuilder1.of(definition,
                  context,
                  mappings,
                  new SchemaConverter1(context, mappings, maxNestingLevel),
                  JavaConverters.asScalaBuffer(containedDefinitions));
        } else {
          throw new IllegalArgumentException(
              "Unsupported encoderVersion: " + encoderVersion + ". Only 1 and 2 are supported.");
        }
        encoderCache.put(key, encoder);
      }

      return encoder;
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
   * Immutable key to look up a matching encoders instance by configuration.
   */
  private static class EncodersKey {

    final FhirVersionEnum fhirVersion;
    final int maxNestingLevel;
    final int encoderVersion;

    EncodersKey(final FhirVersionEnum fhirVersion, int maxNestingLevel, int encoderVersion) {
      this.fhirVersion = fhirVersion;
      this.maxNestingLevel = maxNestingLevel;
      this.encoderVersion = encoderVersion;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      EncodersKey that = (EncodersKey) o;
      return maxNestingLevel == that.maxNestingLevel &&
          encoderVersion == that.encoderVersion &&
          fhirVersion == that.fhirVersion;
    }

    @Override
    public int hashCode() {
      return Objects.hash(fhirVersion, maxNestingLevel, encoderVersion);
    }
  }

  /**
   * Encoder builder. Today only the FHIR version is specified, but future builders may allow
   * customization of the profile used.
   */
  public static class Builder {

    private final static int DEFAULT_ENCODER_VERSION = 2;
    final FhirVersionEnum fhirVersion;
    int maxNestingLevel;
    int encoderVersion;

    Builder(final FhirVersionEnum fhirVersion) {
      this.fhirVersion = fhirVersion;
      this.maxNestingLevel = 0;
      this.encoderVersion = DEFAULT_ENCODER_VERSION;
    }

    /**
     * Set the maximum nesting level for recursive data types. Zero (0) indicates that all direct or
     * indirect fields of type T in element of type T should be skipped.
     *
     * @param maxNestingLevel the maximum nesting level
     * @return this builder
     */
    public Builder withMaxNestingLevel(int maxNestingLevel) {
      this.maxNestingLevel = maxNestingLevel;
      return this;
    }

    public Builder withV1() {
      this.encoderVersion = 1;
      return this;
    }

    public Builder withV2() {
      this.encoderVersion = 2;
      return this;
    }

    /**
     * Get or create an {@link FhirEncoders} instance that matches the builder's configuration.
     *
     * @return an Encoders instance.
     */
    public FhirEncoders getOrCreate() {

      final EncodersKey key = new EncodersKey(fhirVersion, maxNestingLevel, encoderVersion);

      synchronized (ENCODERS) {

        FhirEncoders encoders = ENCODERS.get(key);

        // No instance with the given configuration found,
        // so create one.
        if (encoders == null) {

          final FhirContext context = contextFor(fhirVersion);
          final DataTypeMappings mappings = mappingsFor(fhirVersion);
          encoders = new FhirEncoders(context, mappings, maxNestingLevel, encoderVersion);
          ENCODERS.put(key, encoders);
        }
        return encoders;
      }
    }
  }
}
