/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2022, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 *
 */

package au.csiro.pathling.api.definitions;

import ca.uhn.fhir.context.FhirVersionEnum;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * Helper functions to allow code to convert FHIR resources independently of the FHIR version.
 * Typically an implementation specific to a FHIR version is provided at runtime.
 */
public abstract class FhirConversionSupport implements Serializable {

  private static final long serialVersionUID = -108611742759595166L;

  private static final String R4_SUPPORT_CLASS =
      "au.csiro.pathling.api.definitions.R4FhirConversionSupport";


  /**
   * Returns the type of a given FHIR object, such as "Condition" or "Observation".
   *
   * @param base a FHIR object
   * @return the FHIR type
   */
  public abstract String fhirType(IBase base);

  /**
   * Extracts resources of the given type from a FHIR bundle.
   *
   * @param bundle the bundle
   * @param resourceType the resource type name, such as "Condition" or "Observation"
   * @return the resources of the specified type.
   */
  public abstract List<IBaseResource> extractEntryFromBundle(IBaseBundle bundle,
      String resourceType);

  public abstract IBaseBundle wrapInBundle(IBaseResource... resources);

  /**
   * Cache of FHIR contexts.
   */
  private static final Map<FhirVersionEnum, FhirConversionSupport> FHIR_SUPPORT = new HashMap<>();


  private static FhirConversionSupport newInstance(FhirVersionEnum fhirVersion) {

    Class<? extends FhirConversionSupport> fhirSupportClass;

    if (FhirVersionEnum.R4.equals(fhirVersion)) {

      try {
        //noinspection unchecked
        fhirSupportClass = (Class<? extends FhirConversionSupport>) Class.forName(R4_SUPPORT_CLASS);

      } catch (ClassNotFoundException exception) {
        throw new IllegalStateException(exception);

      }
    } else {
      throw new IllegalArgumentException("Unsupported FHIR version: " + fhirVersion);
    }

    try {

      return fhirSupportClass.getDeclaredConstructor().newInstance();

    } catch (Exception exception) {

      throw new IllegalStateException("Unable to create FHIR support class", exception);
    }
  }

  /**
   * Returns the FHIR context for the given version. This is effectively a cache so consuming code
   * does not need to recreate the context repeatedly.
   *
   * @param fhirVersion the version of FHIR to use
   * @return the FhirContext
   */
  public static FhirConversionSupport supportFor(FhirVersionEnum fhirVersion) {

    synchronized (FHIR_SUPPORT) {

      FhirConversionSupport support = FHIR_SUPPORT.get(fhirVersion);

      if (support == null) {

        support = newInstance(fhirVersion);

        FHIR_SUPPORT.put(fhirVersion, support);
      }

      return support;
    }
  }

  /**
   * Convenience function to load support for FHIR STU3.
   *
   * @return the conversion support instance.
   */
  public static FhirConversionSupport forStu3() {

    return supportFor(FhirVersionEnum.DSTU3);
  }
}
