package au.csiro.pathling.fhirpathe.execution;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ValueFunctions;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.execution.CollectionDataset;
import au.csiro.pathling.fhirpath.execution.DataRoot.ReverseResolveRoot;
import au.csiro.pathling.fhirpath.execution.ExecutorUtils;
import au.csiro.pathling.fhirpath.execution.FhirPathExecutor;
import au.csiro.pathling.fhirpath.execution.SingleFhirPathExecutor;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.path.Paths.EvalFunction;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.assertions.DatasetAssert;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import ca.uhn.fhir.context.FhirContext;
import java.awt.*;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.EpisodeOfCare;
import org.hl7.fhir.r4.model.EpisodeOfCare.EpisodeOfCareStatus;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Reference;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import scala.collection.mutable.WrappedArray;

/**
 * This is a test class to explore issues related to implementation of reverseResolve and resolve
 * functions.
 */
@SpringBootUnitTest
@Slf4j
class FhirpathTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirEncoders encoders;


  final Parser parser = new Parser();

  @Test
  void singleResourceTest() {
    final Patient patient = new Patient();
    patient.setId("1");
    patient.setGender(AdministrativeGender.FEMALE);
    patient.addName().setFamily("Kay").addGiven("Awee");
    patient.addName().setFamily("Kay").addGiven("Zosia");
    final ObjectDataSource dataSource = new ObjectDataSource(spark, encoders,
        List.of(patient));

    final Dataset<Row> result = execFhirPath(
        "where(gender='female').name.where(family.where($this='Kay').exists()).given.join(',')",
        dataSource);
    result.show();
    System.out.println(result.queryExecution().executedPlan().toString());
    final Dataset<Row> expected = DatasetBuilder.of(spark)
        .withColumn("id", DataTypes.StringType)
        .withColumn("value", DataTypes.StringType)
        .withRow("1", "Awee,Zosia")
        .build();

    new DatasetAssert(result)
        .hasRowsUnordered(expected);
  }

  //
  // SECTION: Reverse Resolve
  //

  @Nonnull
  Dataset<Row> evalReverseResolve(@Nonnull final ObjectDataSource dataSource,
      @Nonnull final ReverseResolveRoot joinRoot,
      @Nonnull final String slaveExpressions,
      @Nonnull final Function<Column, Column> aggFunction) {
    System.out.println("ReverseResolve: " + joinRoot.getTag() + "->" + slaveExpressions);

    final CollectionDataset parentResult = evalFhirPath(joinRoot.getMasterResourceType(),
        "id",
        dataSource);
    // ignore value here - we could just use the resource dataset directly
    // TODO: get access  to it
    final Dataset<Row> parentDataset = parentResult.getDataset();
    parentDataset.show();
    final CollectionDataset childResult = evalFhirPath(joinRoot.getForeignResourceType(),
        slaveExpressions,
        dataSource);

    // TODO:  We should be able to combine the evaluation actually 
    // either by having a function that evaluates multiple expressions or 
    // having the dataset returne from the evaluator
    // for now lest just ignore the dataset and only use the value column

    final CollectionDataset childParentKeyResult = evalFhirPath(joinRoot.getForeignResourceType(),
        joinRoot.getForeignKeyPath() + "." + "reference",
        dataSource);

    final Dataset<Row> childDataset = childResult.getDataset().select(
        childParentKeyResult.getValueColumn().alias(joinRoot.getForeignKeyTag()),
        childResult.getValueColumn().alias(joinRoot.getValueTag()));
    childDataset.show();

    final Dataset<Row> foreignResult = childDataset.groupBy(
            functions.col(joinRoot.getForeignKeyTag()))
        .agg(aggFunction.apply(functions.col(joinRoot.getValueTag()))
            .alias(joinRoot.getValueTag()));
    foreignResult.show();

    final Dataset<Row> joinedResult = parentDataset.join(foreignResult,
        parentDataset.col("key").equalTo(foreignResult.col(joinRoot.getForeignKeyTag())),
        "left_outer");
    joinedResult.show();
    return joinedResult.select(functions.col("id"),
        functions.col(joinRoot.getValueTag()).alias("value"));
  }

  @Nonnull
  static Function<Column, Column> getAggregation(@Nonnull final FhirPath path) {
    if (path instanceof Composite) {
      throw new IllegalArgumentException("Simple path expected");
    }
    if (path instanceof final EvalFunction evalFunction) {
      if (evalFunction.getFunctionIdentifier().equals("count")) {
        return functions::sum;
      }
    }
    return c -> ValueFunctions.unnest(functions.collect_list(c));
  }


  @Nonnull
  static Optional<EvalFunction> asReverseResolve(final FhirPath path) {
    if (path instanceof EvalFunction evalFunction && evalFunction.getFunctionIdentifier()
        .equals("reverseResolve")) {
      return Optional.of(evalFunction);
    }
    return Optional.empty();
  }

  @Nonnull
  Dataset<Row> evalReverseResolve(@Nonnull final ObjectDataSource dataSource,
      @Nonnull final ResourceType subjectResource,
      @Nonnull final String reverseResolveExpression) {
    final FhirPath reverseResolvePath = parser.parse(reverseResolveExpression);
    System.out.println(reverseResolvePath);
    final EvalFunction reverseResolve = asReverseResolve(
        reverseResolvePath.first()).orElseThrow();
    System.out.println(reverseResolve);
    final ReverseResolveRoot joinRoot = ExecutorUtils.fromPath(subjectResource,
        reverseResolve);
    System.out.println(joinRoot);
    final FhirPath childPath = reverseResolvePath.suffix();
    // check if the child ends with an aggregate function
    // TODO: this actually is more complex and should look for the first occurence of an aggregate function
    // TODO: make eval work with FhirPath rather then string expressions
    return evalReverseResolve(dataSource, joinRoot, childPath
        .toExpression(), getAggregation(childPath.last()));
  }

  @Test
  void simpleReverseResolveToSingularValue() {
    final ObjectDataSource dataSource = new ObjectDataSource(spark, encoders,
        List.of(
            new Patient().setGender(AdministrativeGender.FEMALE).setId("Patient/1"),
            new Patient().setGender(AdministrativeGender.MALE).setId("Patient/2"),
            new Condition().setSubject(new Reference("Patient/1")).setId("Condition/x"),
            new Condition().setSubject(new Reference("Patient/1")).setId("Condition/y")
        ));
    final Dataset<Row> resultDataset = evalReverseResolve(dataSource, ResourceType.PATIENT,
        "reverseResolve(Condition.subject).id");
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    resultDataset.show();
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", WrappedArray.make(new String[]{"x", "y"})),
            RowFactory.create("2", null)
        );
  }

  @Test
  void simpleReverseResolveToManyValue() {
    final ObjectDataSource dataSource = getPatientsWithConditions();
    final Dataset<Row> resultDataset = evalReverseResolve(dataSource, ResourceType.PATIENT,
        "reverseResolve(Condition.subject).code.coding.code");
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", sql_array("code-xx", "code-xy", "code-yx", "code-yy")),
            RowFactory.create("2", null)
        );
  }

  @Test
  void simpleReverseResolveToLeafAggregateFunction() {
    final ObjectDataSource dataSource = getPatientsWithConditions();

    // final ReverseResolveRoot joinRoot = ReverseResolveRoot.ofResource(ResourceType.PATIENT,
    //     ResourceType.CONDITION, "subject");
    // final Dataset<Row> resultDataset = evalReverseResolve(dataSource, joinRoot,
    //     "code.coding.code.count()", functions::sum);
    final Dataset<Row> resultDataset = evalReverseResolve(dataSource, ResourceType.PATIENT,
        "reverseResolve(Condition.subject).code.coding.code.count()");

    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", 4),
            RowFactory.create("2", null)
        );
  }

  //
  // SECTION: Resolve
  //
  @Test
  void resolveManyToOneWithSimpleValue() {
    // ON Condition
    //final Dataset<Row> joinedResult = evalFhirPathMulti(ResourceType.CONDITION, 
    // "subject.resolve().name.gender", dataSource);
    //joinedResult.show();
    final ObjectDataSource dataSource = getPatientsWithConditions();
    final CollectionDataset conditionResult = evalFhirPath(ResourceType.CONDITION,
        "subject",
        dataSource);

    final Dataset<Row> conditionDataset = conditionResult
        .getDataset()
        .withColumn("_Patient_key", conditionResult.getValueColumn().getField("reference"));
    conditionDataset.show();

    final CollectionDataset patientResult = evalFhirPath(ResourceType.PATIENT, "gender",
        dataSource);
    // ignore value here - we could just use the resource dataset directly
    // TODO: get access  to it
    final Dataset<Row> patientDataset = patientResult.materialize("_Patient_fv_01");
    patientDataset.show();

    // TODO: add unique join id and as a prefix to related columns
    // also possibly wrap the value in a struct to allow for multiple results from the same join
    final Dataset<Row> joinedResult = conditionDataset.join(
        patientDataset.select("key", "_Patient_fv_01"),
        conditionDataset.col("_Patient_key").equalTo(patientDataset.col("key")), "left_outer");
    joinedResult.show();

    final Dataset<Row> finalResult = joinedResult.select(
        functions.col("id"),
        functions.col("_Patient_fv_01").alias("value"));

    finalResult.show();
    System.out.println(finalResult.queryExecution().executedPlan().toString());
    new DatasetAssert(finalResult)
        .hasRowsUnordered(
            RowFactory.create("x", "female"),
            RowFactory.create("y", "female")
        );
  }

  @Test
  void resolveToManyWithSimpleValue() {
    // ON Encounter
    // final Dataset<Row> joinedResult = evalFhirPathMulti(ResourceType.ENCOUNTER,"episodeOfCare.resolve().status", dataSource);

    final ObjectDataSource dataSource = new ObjectDataSource(spark, encoders,
        List.of(
            new Encounter()
                .addEpisodeOfCare(new Reference("EpisodeOfCare/01_1"))
                .addEpisodeOfCare(new Reference("EpisodeOfCare/01_2"))
                .setId("Encounter/01"),
            new Encounter()
                .addEpisodeOfCare(new Reference("EpisodeOfCare/02_1"))
                .addEpisodeOfCare(new Reference("EpisodeOfCare/02_2"))
                .setId("Encounter/02"),
            new Encounter().setId("Encounter/03"),
            new EpisodeOfCare().setStatus(EpisodeOfCareStatus.ACTIVE).setId("EpisodeOfCare/01_1"),
            new EpisodeOfCare().setStatus(EpisodeOfCareStatus.FINISHED)
                .setId("EpisodeOfCare/01_2"),
            new EpisodeOfCare().setStatus(EpisodeOfCareStatus.ONHOLD).setId("EpisodeOfCare/02_1"),
            new EpisodeOfCare().setStatus(EpisodeOfCareStatus.PLANNED).setId("EpisodeOfCare/02_2")
        ));

    final CollectionDataset eocResult = evalFhirPath(ResourceType.EPISODEOFCARE,
        "status",
        dataSource);

    final Dataset<Row> eocDataset = eocResult.materialize("_EpisodeOfCare_fv_01");
    eocDataset.show();

    final CollectionDataset encounterResult = evalFhirPath(ResourceType.ENCOUNTER,
        "episodeOfCare",
        dataSource);

    final Dataset<Row> encounterDataset = encounterResult
        .getDataset()
        .withColumn("_EOC_keys", encounterResult.getValueColumn().getField("reference"));
    encounterDataset.show();

    // this is tricky for many reasons but as unless  we incoroprate the concept of exploded collection 
    // we need to immediately group the foreign values to a list (somehow correlated with the original references)

    final Dataset<Row> explodedDataset = encounterDataset
        .select(functions.col("*"), functions.posexplode_outer(encounterDataset.col("_EOC_keys"))
            .as(new String[]{"_EOC_pos", "_EOC_key"}))
        .drop("_EOC_keys");

    explodedDataset.show();

    final Dataset<Row> joinedResult = explodedDataset.join(
        eocDataset.select(eocDataset.col("key").alias("_EOC_key_slave"),
            eocDataset.col("_EpisodeOfCare_fv_01")),
        explodedDataset.col("_EOC_key").equalTo(functions.col("_EOC_key_slave")), "left_outer");

    joinedResult.show();

    // TOOD: the order needs to preserved here by using structs of (pos,value) and then sorting it back and flattening

    final Dataset<Row> groupedResult = joinedResult.groupBy(
        functions.col("id"),
        functions.col("key")
    ).agg(
        functions.any_value(joinedResult.col("Encounter")).alias("Encounter"),
        functions.any_value(joinedResult.col("_fid")).alias("_fid"),
        functions.any_value(joinedResult.col("_extension")).alias("_extension"),
        functions.collect_list(functions.col("_EpisodeOfCare_fv_01"))
            .alias("_EpisodeOfCare_fv_01")
    );
    groupedResult.show();

    final Dataset<Row> finalResult = groupedResult.select(
        functions.col("id"),
        functions.col("_EpisodeOfCare_fv_01").alias("value"));
    finalResult.show();
    System.out.println(finalResult.queryExecution().executedPlan().toString());

    new DatasetAssert(finalResult)
        .hasRowsUnordered(
            RowFactory.create("01", sql_array("active", "finished")),
            RowFactory.create("02", sql_array("onhold", "planned")),
            RowFactory.create("03", sql_array())
        );
  }

  @Nonnull
  private ObjectDataSource getPatientsWithConditions() {
    return new ObjectDataSource(spark, encoders,
        List.of(
            new Patient().setGender(AdministrativeGender.FEMALE).setId("Patient/1"),
            new Patient().setGender(AdministrativeGender.MALE).setId("Patient/2"),
            new Condition()
                .setSubject(new Reference("Patient/1"))
                .setCode(
                    new CodeableConcept()
                        .addCoding(new Coding().setCode("code-xx"))
                        .addCoding(new Coding().setCode("code-xy"))
                        .setText("Coding-x")
                )
                .setId("Condition/x"),
            new Condition()
                .setSubject(new Reference("Patient/1"))
                .setCode(
                    new CodeableConcept()
                        .addCoding(new Coding().setCode("code-yx"))
                        .addCoding(new Coding().setCode("code-yy"))
                        .setText("Coding-y")
                )
                .setId("Condition/y")
        ));
  }

  @Nonnull
  private static <T> WrappedArray<T> sql_array(final T... values) {
    return WrappedArray.make(values);
  }

  @Nonnull
  private Dataset<Row> execFhirPath(final ResourceType subjectResourceType,
      final String fhirpathExpression,
      final ObjectDataSource dataSource) {
    final FhirPath path = parser.parse(fhirpathExpression);
    System.out.println(path.toExpression());
    final FhirPathExecutor executor = new SingleFhirPathExecutor(subjectResourceType,
        FhirContext.forR4(), StaticFunctionRegistry.getInstance(),
        Collections.emptyMap(), dataSource);

    return executor.execute(path);
  }

  @Nonnull
  private CollectionDataset evalFhirPath(final ResourceType subjectResourceType,
      final String fhirpathExpression,
      final ObjectDataSource dataSource) {
    final FhirPath path = parser.parse(fhirpathExpression);
    System.out.println("Eval: " + subjectResourceType.toCode() + "." + path.toExpression());
    final FhirPathExecutor executor = new SingleFhirPathExecutor(subjectResourceType,
        FhirContext.forR4(), StaticFunctionRegistry.getInstance(),
        Collections.emptyMap(), dataSource);

    return executor.evaluate(path);
  }

  @Nonnull
  private Dataset<Row> execFhirPath(final String fhirpathExpression,
      final ObjectDataSource dataSource) {
    return execFhirPath(ResourceType.PATIENT, fhirpathExpression, dataSource);
  }
}
