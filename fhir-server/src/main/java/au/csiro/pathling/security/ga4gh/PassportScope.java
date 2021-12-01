/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security.ga4gh;

import java.util.HashMap;
import java.util.Set;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;
import org.springframework.web.context.WebApplicationContext;

/**
 * Stores a set of FHIRPath filter expressions that define a permitted scope for the current client,
 * based upon the GA4GH passport they presented.
 *
 * @author John Grimes
 */
@Component
@ConditionalOnProperty(prefix = "pathling", name = {"auth.enabled", "auth.ga4gh-passports.enabled"},
    havingValue = "true")
@Profile("server")
@Scope(value = WebApplicationContext.SCOPE_REQUEST, proxyMode = ScopedProxyMode.TARGET_CLASS)
public class PassportScope extends HashMap<ResourceType, Set<String>> {

  private static final long serialVersionUID = -6584811334567819168L;

}
