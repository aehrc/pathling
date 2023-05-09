package au.csiro.pathling.examples;

import au.csiro.pathling.library.PathlingContext;
import ca.uhn.fhir.parser.IParser;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.file.Path;
import java.util.Objects;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Base;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Property;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Resource;

import static java.util.function.Predicate.not;

public class EncodeBundles {


  static boolean isURNReference(final Reference reference) {
    return reference.hasReference() && reference.getReference().startsWith("urn:");
  }

  static void rewriteURNReference(final Reference reference) {
    final IBaseResource resource = reference.getResource();
    if (isURNReference(reference) && resource != null) {
      reference.setReference(resource.getIdElement().getValue());
    }
  }

  static void rewriteURNReferences(final Base object) {
    object.children().stream()
        .flatMap(p -> p.getValues().stream())
        .filter(Objects::nonNull)
        .filter(not(Base::isPrimitive))
        .forEach(EncodeBundles::processTypeValue);
  }

  static void processTypeValue(final Base object) {
    if (object instanceof Reference) {
      rewriteURNReference((Reference) object);
    }
    rewriteURNReferences(object);
  }

  public static void main(final String[] args) throws FileNotFoundException {

    final Path bundlesData = Path.of("library-api/src/test/resources/test-data/references").toAbsolutePath();
    final Path ndjsonData = Path.of("lib/python/examples/data/resources").toAbsolutePath();
    System.out.printf("Bundles Data: %s\n", bundlesData);

    final SparkSession spark = SparkSession.builder()
        .appName(EncodeBundles.class.getName())
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate();

    final PathlingContext ptc = PathlingContext.create(spark);

    // final Dataset<Row> bundlesDataset = spark.read().option("wholetext", true)
    //     .text(bundlesData.toString());
    // bundlesDataset.show();
    //
    // final Dataset<Row> conditionsFromBundleDS = ptc.encodeBundle(bundlesDataset, "Condition");
    // conditionsFromBundleDS.show();
    //
    final Dataset<Row> conditionsFromNdjsonDS = ptc.encode(spark.read().text(ndjsonData.toString()), "Patient");
    conditionsFromNdjsonDS.show();

    final IParser jsonParser = ptc.getFhirContext().newJsonParser();
    jsonParser.setOverrideResourceIdWithBundleEntryFullUrl(false);

    final Bundle bundle = (Bundle) jsonParser.parseResource(
        new FileReader(bundlesData.resolve(
            "test-bundle.json").toString()));

    System.out.println(bundle);
    bundle.getEntry().stream()
        .map(Bundle.BundleEntryComponent::getResource)
        .forEach(EncodeBundles::rewriteURNReferences);

    // // find references
    // final Resource resource0 = bundle.getEntry().get(2).getResource();
    //
    // // final RuntimeResourceDefinition definition = FhirContext.forR4()
    // //     .getResourceDefinition(resource0);
    // // definition.getChildren().forEach(System.out::println);
    // // RuntimeChildResourceDefinition childByName = (RuntimeChildResourceDefinition) definition.getChildByName("subject");
    // // System.out.println(ToStringBuilder.reflectionToString(childByName));
    //
    // final Property subject = resource0.getChildByName("subject");
    // resource0.children();
    // System.out.println(ToStringBuilder.reflectionToString(subject));
    // resource0.children().stream()
    //     .flatMap(p -> p.getValues().stream())
    //     .filter(Reference.class::isInstance)
    //     .map(Reference.class::cast)
    //     .forEach(r -> {
    //       final IBaseResource resource = r.getResource();
    //       if (resource != null) {
    //         r.setReference(resource.getIdElement().getValue());
    //       }
    //     });
    jsonParser.setPrettyPrint(true);
    System.out.println(jsonParser.encodeResourceToString(bundle));
  }
}
