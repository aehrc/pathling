/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security.ga4gh;

import java.util.List;
import lombok.Data;

/**
 * Represents the JSON manifest that describes a GA4GH visa.
 *
 * @author John Grimes
 * @see <a href="https://github.com/ga4gh-duri/ga4gh-duri.github.io/blob/master/researcher_ids/ga4gh_passport_v1.md">GA4GH
 * Passport</a>
 */
@Data
public class VisaManifest {

  List<String> patientIds;

}
