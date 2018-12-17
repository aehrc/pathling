/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */
package au.csiro.clinsight.fhir;

import org.hl7.fhir.dstu3.model.Reference;

/**
 * @author John Grimes
 */
public abstract class FhirUtilities {

    public static String getIdFromReference(Reference reference) {
        if (reference == null || reference.getReference() == null)
            throw new IllegalArgumentException("Cannot get ID from null Reference.");
        return reference.getReference().split("/")[1];
    }

}
