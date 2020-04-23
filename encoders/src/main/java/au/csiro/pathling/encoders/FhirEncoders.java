/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.encoders;

import au.csiro.pathling.encoders.datatypes.DataTypeMappings;
import ca.uhn.fhir.context.BaseRuntimeElementCompositeDefinition;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import java.util.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.instance.model.api.IBaseResource;
import scala.collection.JavaConversions;

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
  private static final Map<FhirVersionEnum, DataTypeMappings> DATA_TYPE_MAPPINGS = new HashMap();

  /**
   * Cache of FHIR contexts.
   */
  private static final Map<FhirVersionEnum, FhirContext> FHIR_CONTEXTS = new HashMap();

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
   * Consumers should generally use the {@link #forR4()} method, but this is made available for test
   * purposes and additional experimental mappings.
   *
   * @param context the FHIR context to use.
   * @param mappings mappings between Spark and FHIR data types.
   */
  public FhirEncoders(FhirContext context, DataTypeMappings mappings) {
    this.context = context;
    this.mappings = mappings;
  }

  /**
   * Returns the FHIR context for the given version. This is effectively a cache so consuming code
   * does not need to recreate the context repeatedly.
   *
   * @param fhirVersion the version of FHIR to use
   * @return the FhirContext
   */
  public static FhirContext contextFor(FhirVersionEnum fhirVersion) {

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
  static DataTypeMappings mappingsFor(FhirVersionEnum fhirVersion) {

    synchronized (DATA_TYPE_MAPPINGS) {

      DataTypeMappings mappings = DATA_TYPE_MAPPINGS.get(fhirVersion);

      if (mappings == null) {
        String dataTypesClassName;

        if (fhirVersion == FhirVersionEnum.R4) {
          dataTypesClassName = "au.csiro.pathling.encoders.datatypes.R4DataTypeMappings";
        } else {
          throw new IllegalArgumentException("Unsupported FHIR version: " + fhirVersion);
        }

        try {

          mappings = (DataTypeMappings) Class.forName(dataTypesClassName).newInstance();

          DATA_TYPE_MAPPINGS.put(fhirVersion, mappings);

        } catch (Exception createClassException) {

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
  public static Builder forVersion(FhirVersionEnum fhirVersion) {
    return new Builder(fhirVersion);
  }

  /**
   * Returns an encoder for the given FHIR resource.
   *
   * @param type the type of the resource to encode.
   * @param <T> the type of the resource to be encoded.
   * @return an encoder for the resource.
   */
  public final <T extends IBaseResource> ExpressionEncoder<T> of(String type) {

    return of(type, new String[]{});
  }

  /**
   * Returns an encoder for the given FHIR resource.
   *
   * @param type the type of the resource to encode.
   * @param <T> the type of the resource to be encoded.
   * @return an encoder for the resource.
   */
  public final <T extends IBaseResource> ExpressionEncoder<T> of(Class<T> type) {

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
  public <T extends IBaseResource> ExpressionEncoder<T> of(String resourceName,
      String... contained) {

    RuntimeResourceDefinition definition = context.getResourceDefinition(resourceName);

    List<Class<? extends IBaseResource>> containedClasses = new ArrayList<>();

    for (String containedName : contained) {

      containedClasses.add(context.getResourceDefinition(containedName).getImplementingClass());
    }

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
  public final <T extends IBaseResource> ExpressionEncoder<T> of(Class<T> type,
      Class... contained) {

    List<Class<? extends IBaseResource>> containedResourceList = new ArrayList<>();

    for (Class element : contained) {

      if (IBaseResource.class.isAssignableFrom(element)) {

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
  public final <T extends IBaseResource> ExpressionEncoder<T> of(Class<T> type,
      List<Class<? extends IBaseResource>> contained) {

    BaseRuntimeElementCompositeDefinition definition =
        context.getResourceDefinition(type);

    List<BaseRuntimeElementCompositeDefinition<?>> containedDefinitions = new ArrayList<>();

    for (Class<? extends IBaseResource> resource : contained) {

      containedDefinitions.add(context.getResourceDefinition(resource));
    }

    StringBuilder keyBuilder = new StringBuilder(type.getName());

    for (Class resource : contained) {

      keyBuilder.append(resource.getName());
    }

    int key = keyBuilder.toString().hashCode();

    synchronized (encoderCache) {

      ExpressionEncoder<T> encoder = encoderCache.get(key);

      if (encoder == null) {

        encoder = (ExpressionEncoder<T>)
            EncoderBuilder.of(definition,
                context,
                mappings,
                new SchemaConverter(context, mappings),
                JavaConversions.asScalaBuffer(containedDefinitions));

        encoderCache.put(key, encoder);
      }

      return encoder;
    }
  }

  /**
   * Immutable key to look up a matching encoders instance by configuration.
   */
  private static class EncodersKey {

    final FhirVersionEnum fhirVersion;

    EncodersKey(FhirVersionEnum fhirVersion) {
      this.fhirVersion = fhirVersion;
    }

    @Override
    public int hashCode() {
      return fhirVersion.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof EncodersKey)) {
        return false;
      }

      EncodersKey that = (EncodersKey) obj;

      return this.fhirVersion == that.fhirVersion;
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
   * Encoder builder. Today only the FHIR version is specified, but future builders may allow
   * customization of the profile used.
   */
  public static class Builder {

    final FhirVersionEnum fhirVersion;

    Builder(FhirVersionEnum fhirVersion) {

      this.fhirVersion = fhirVersion;
    }

    /**
     * Get or create an {@link FhirEncoders} instance that matches the builder's configuration.
     *
     * @return an Encoders instance.
     */
    public FhirEncoders getOrCreate() {

      EncodersKey key = new EncodersKey(fhirVersion);

      synchronized (ENCODERS) {

        FhirEncoders encoders = ENCODERS.get(key);

        // No instance with the given configuration found,
        // so create one.
        if (encoders == null) {

          FhirContext context = contextFor(fhirVersion);
          DataTypeMappings mappings = mappingsFor(fhirVersion);

          encoders = new FhirEncoders(context, mappings);

          ENCODERS.put(key, encoders);
        }

        return encoders;
      }
    }
  }
}
