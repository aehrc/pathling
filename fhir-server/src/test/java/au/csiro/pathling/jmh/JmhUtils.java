package au.csiro.pathling.jmh;

import org.openjdk.jmh.results.format.ResultFormatType;
import javax.annotation.Nonnull;
import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class JmhUtils {

  private JmhUtils() {
  }

  @Nonnull
  public static String buildResultsFileName(@Nonnull final String outputDir,
      @Nonnull final String resultFilePrefix, @Nonnull final ResultFormatType resultType) {
    LocalDateTime date = LocalDateTime.now();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd-hhmmss");

    String suffix;
    switch (resultType) {
      case CSV:
        suffix = ".csv";
        break;
      case SCSV:
        // Semi-colon separated values
        suffix = ".scsv";
        break;
      case LATEX:
        suffix = ".tex";
        break;
      case JSON:
      default:
        suffix = ".json";
        break;

    }
    return new File(new File(outputDir),
        String.format("%s%s%s", resultFilePrefix, date.format(formatter), suffix)).getPath();
  }


}
