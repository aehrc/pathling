package au.csiro.pathling.operations.bulkexport;

import java.io.Serial;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @author Felix Naumann
 */
@Slf4j
@Component
public class ExportResultRegistry extends ConcurrentHashMap<String, ExportResult> {

  @Serial
  private static final long serialVersionUID = -3960163244304628646L;
}
