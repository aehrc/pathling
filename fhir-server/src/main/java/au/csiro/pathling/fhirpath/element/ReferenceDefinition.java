/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.element;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import ca.uhn.fhir.context.RuntimeChildResourceDefinition;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Encapsulates the FHIR definitions for a reference element.
 *
 * @author John Grimes
 */
public class ReferenceDefinition extends ElementDefinition {

  @Nonnull
  private final RuntimeChildResourceDefinition childDefinition;

  protected ReferenceDefinition(@Nonnull final RuntimeChildResourceDefinition childDefinition,
      @Nonnull final String elementName) {
    super(childDefinition, elementName);
    this.childDefinition = childDefinition;
  }

  @Override
  @Nonnull
  public Set<ResourceType> getReferenceTypes() {
    final List<Class<? extends IBaseResource>> resourceTypes = childDefinition.getResourceTypes();
    checkNotNull(resourceTypes);

    return resourceTypes.stream()
        .map(clazz -> {
          final String resourceCode;
          try {
            if (clazz.getName().equals("org.hl7.fhir.instance.model.api.IAnyResource")) {
              return Enumerations.ResourceType.RESOURCE;
            } else {
              resourceCode = clazz.getConstructor().newInstance().fhirType();
              return Enumerations.ResourceType.fromCode(resourceCode);
            }
          } catch (final InstantiationException | IllegalAccessException |
                         InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException("Problem accessing resource types on element", e);
          }
        })
        .collect(Collectors.toSet());
  }

}
