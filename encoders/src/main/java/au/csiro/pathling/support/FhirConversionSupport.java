/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2022 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.support;

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
      "au.csiro.pathling.support.r4.R4FhirConversionSupport";

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
   * @param resourceClass the class of the resources to extract
   * @param <T> the type of the resources to extract
   * @return the list of the resources of the specified type
   */
  public abstract <T extends IBaseResource> List<IBaseResource> extractEntryFromBundle(
      IBaseBundle bundle,
      Class<T> resourceClass);

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
      return FHIR_SUPPORT.computeIfAbsent(fhirVersion, FhirConversionSupport::newInstance);
    }
  }
}
