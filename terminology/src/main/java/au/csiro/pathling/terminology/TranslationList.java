package au.csiro.pathling.terminology;

import au.csiro.pathling.terminology.TerminologyService.Translation;
import java.io.Serial;
import java.util.ArrayList;

/**
 * A list of translations, used to represent the results of a terminology translation operation.
 * This class extends {@link ArrayList} to provide a convenient way to handle collections of
 * translations.
 *
 * @author John Grimes
 */
public class TranslationList extends ArrayList<Translation> {

  @Serial
  private static final long serialVersionUID = 6661704514488108566L;
}
