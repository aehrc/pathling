package au.csiro.pathling.views;

import static au.csiro.pathling.utilities.Functions.backCurried;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import au.csiro.pathling.encoders.ValueFunctions;
import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.fhirpath.parser.ConstantReplacer;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Executes a FHIR view query.
 *
 * @author John Grimes
 */
public class FhirViewExecutor {

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final SparkSession sparkSession;

  @Nonnull
  private final DataSource dataSource;

  @Nonnull
  private final Optional<TerminologyServiceFactory> terminologyServiceFactory;


  @Value
  private static class ExecutionContextImpl implements ExecutionContext {

    EvaluationContext evaluationContext;
    DatasetContext datasetContext;

    @Nonnull
    @Override
    public Column evalExpression(@Nonnull final String path, final boolean singularise) {
      return singularise
             ? internalEvalExpression(path).getSingleton()
             : internalEvalExpression(path).getColumn();
    }

    @Nonnull
    @Override
    public ExecutionContextImpl subContext(@Nonnull final String expression, final boolean unnest) {
      final Collection newInputContext = internalEvalExpression(expression);
      return new ExecutionContextImpl(
          evaluationContext.withInputContext(unnest
                                             ? unnest(newInputContext)
                                             : newInputContext),
          datasetContext);
    }


    @Nonnull
    private Collection internalEvalExpression(@Nonnull final String expression) {
      final String updatedExpression = evaluationContext.getConstantReplacer()
          .map(replacer -> replacer.execute(expression))
          .orElse(expression);
      return new Parser().evaluate(updatedExpression, evaluationContext);
    }

    @Nonnull
    private Collection unnest(@Nonnull final Collection collection) {
      return collection.copyWith(
          datasetContext.materialize(collection.getCtx().explode().getValue()));
    }

    @Nonnull
    Dataset<Row> getDataset() {
      return datasetContext.getDataset();
    }
  }

  public FhirViewExecutor(@Nonnull final FhirContext fhirContext,
      @Nonnull final SparkSession sparkSession, @Nonnull final DataSource dataset,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory) {
    this.fhirContext = fhirContext;
    this.sparkSession = sparkSession;
    this.dataSource = dataset;
    this.terminologyServiceFactory = terminologyServiceFactory;
  }

  @Nonnull
  public Dataset<Row> buildQuery(@Nonnull final FhirView view) {

    final ExecutionContextImpl executionContext = newExecutionContext(view);
    final List<Column> select = parseSelect(view.getSelect(), executionContext);

    final List<String> where = view.getWhere() == null
                               ? Collections.emptyList()
                               : view.getWhere().stream()
                                   .map(WhereClause::getExpression)
                                   .collect(toList());
    final Optional<Column> filterCondition = where.stream()
        .map(executionContext::evalExpression)
        .reduce(Column::and);

    // TODO: Make the context immutable
    // NOTE: The executionContext is mutable with regards to the dataset
    // Due to the necessity or column "materializations" needed for unnesting.
    final Dataset<Row> dataset = executionContext.getDataset();
    return filterCondition
        .map(dataset::filter)
        .orElse(dataset)
        .select(select.toArray(new Column[0]));
  }


  private ExecutionContextImpl newExecutionContext(@Nonnull final FhirView view) {
    final Optional<ConstantReplacer> constantReplacer = Optional.ofNullable(view.getConstants())
        .map(c -> c.stream()
            .collect(toMap(ConstantDeclaration::getName, ConstantDeclaration::getValue)))
        .map(ConstantReplacer::new);

    // Build a new expression parser, and parse all the column expressions within the query.
    final ResourceType resourceType = ResourceType.fromCode(view.getResource());
    final Dataset<Row> dataset = dataSource.read(resourceType);
    final ResourceCollection inputContext = ResourceCollection.build(fhirContext, dataset,
        resourceType, false);
    final EvaluationContext evaluationContext = new EvaluationContext(inputContext, inputContext,
        fhirContext, sparkSession, dataset, StaticFunctionRegistry.getInstance(),
        terminologyServiceFactory, constantReplacer);
    return new ExecutionContextImpl(evaluationContext,
        new DatasetContext(dataset));
  }

  @Nonnull
  private List<Column> parseSelect(@Nonnull final List<SelectClause> selectGroup,
      @Nonnull final ExecutionContext context) {

    return selectGroup.stream()
        .flatMap(select -> evalSelect(select, context).stream())
        .collect(Collectors.toUnmodifiableList());
  }

  @Nonnull
  private List<Column> evalSelect(@Nonnull final SelectClause select,
      @Nonnull final ExecutionContext context) {
    // TODO: move to the classes ???
    if (select instanceof DirectSelection) {
      return directSelection(context, (DirectSelection) select);
    } else if (select instanceof FromSelection) {
      return nestedSelection(context, (FromSelection) select, false);
    } else if (select instanceof ForEachSelection) {
      return nestedSelection(context, (ForEachSelection) select, true);
    } else if (select instanceof UnionSelection) {
      return unionSelection(context, (UnionSelection) select);
    } else {
      throw new IllegalStateException("Unknown select clause type: " + select.getClass());
    }
  }

  @Nonnull
  private List<Column> unionSelection(@Nonnull final ExecutionContext context,
      @Nonnull final UnionSelection select) {
    final List<Column> unionColumns = nestedSelection(context, select, false);
    // need to combine all these columns here
    return List.of(functions.flatten(functions.array(
        unionColumns.stream()
            .map(c -> ValueFunctions.ifArray(c, Function.<Column>identity()::apply,
                functions::array))
            .toArray(Column[]::new)
    )));
  }

  @Nonnull
  private List<Column> directSelection(@Nonnull final ExecutionContext context,
      @Nonnull final DirectSelection select) {
    return List.of(
        context.evalExpression(select.getPath(), !select.isCollection(),
            Optional.ofNullable(select.getAlias())));
  }

  @Nonnull
  @NotImplemented
  private List<Column> nestedSelection(final @Nonnull ExecutionContext context,
      @Nonnull final NestedSelectClause select,
      final boolean unnest) {

    final ExecutionContext nextContext = Optional.ofNullable(select.getPath())
        .map(backCurried(context::subContext).apply(unnest))
        .orElse(context);

    return parseSelect(select.getSelect(), nextContext);

  }
}
