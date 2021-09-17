/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.async;

import java.util.concurrent.ConcurrentHashMap;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Used to store a mapping between stages and the job that they are related to, for the purpose of
 * monitoring progress.
 *
 * @author John Grimes
 */
@Component
@Profile("server")
@ConditionalOnProperty(prefix = "pathling", name = "async.enabled", havingValue = "true")
public class StageMap extends ConcurrentHashMap<Integer, String> {

  private static final long serialVersionUID = -8385386725080021575L;

}
