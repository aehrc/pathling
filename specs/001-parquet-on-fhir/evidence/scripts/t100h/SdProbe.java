import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.test.layout.LayoutDatasets;
import au.csiro.pathling.test.layout.TestLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.SparkSession;

/** Throwaway probe for T100h: reading a StructureDefinition on each layout. */
public class SdProbe {
  public static void main(final String[] args) throws Exception {
    final SparkSession spark =
        SparkSession.builder().master("local[1]").config("spark.ui.enabled", false).getOrCreate();
    final FhirEncoders encoders =
        FhirEncoders.forR4().withExtensionsEnabled(true).withAllOpenTypes().getOrCreate();
    final String json = Files.readString(Path.of(args[0])).replaceAll("\\s*\\n\\s*", " ");
    for (final TestLayout layout : List.of(TestLayout.PREVIOUS, TestLayout.POF)) {
      try {
        final long n = LayoutDatasets.fromJson(spark, encoders, layout, "StructureDefinition", List.of(json)).count();
        System.out.println(layout + " | read " + n + " row(s)");
      } catch (final Throwable t) {
        System.out.println(layout + " | ERROR " + t);
      }
    }
    spark.stop();
  }
}
