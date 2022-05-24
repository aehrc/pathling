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

package au.csiro.pathling.support.r4;

import au.csiro.pathling.support.FhirConversionSupport;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;

import java.util.List;
import java.util.stream.Collectors;

public class R4FhirConversionSupport extends FhirConversionSupport {

    private static final long serialVersionUID = -367070946615790595L;

    @Override
    public String fhirType(IBase base) {

        return base.fhirType();
    }

    @Override
    public <T extends IBaseResource> List<IBaseResource> extractEntryFromBundle(IBaseBundle bundle, Class<T> resourceClass) {
        Bundle r4Bundle = (Bundle) bundle;

        return r4Bundle.getEntry().stream()
                .map(BundleEntryComponent::getResource)
                .filter(resourceClass::isInstance)
                .collect(Collectors.toList());
    }
}

