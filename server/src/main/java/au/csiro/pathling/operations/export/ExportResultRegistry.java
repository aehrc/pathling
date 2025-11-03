package au.csiro.pathling.operations.export;

import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * @author Felix Naumann
 */
@Slf4j
@Component
@Profile("core")
public class ExportResultRegistry extends ConcurrentHashMap<String, ExportResult> {

  private static final long serialVersionUID = -3960163244304628646L;
}
