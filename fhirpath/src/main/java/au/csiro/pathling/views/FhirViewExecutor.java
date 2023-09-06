package au.csiro.pathling.views;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.fhirpath.parser.ConstantReplacer;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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


  public FhirViewExecutor(@Nonnull final FhirContext fhirContext,
      @Nonnull final SparkSession sparkSession, @Nonnull final DataSource dataSource,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory) {
    this.fhirContext = fhirContext;
    this.sparkSession = sparkSession;
    this.dataSource = dataSource;
    this.terminologyServiceFactory = terminologyServiceFactory;
  }

  @Nonnull
  public Dataset<Row> buildQuery(@Nonnull final FhirView view) {
    // Get the constants from the query, and build a replacer.
    final Optional<ConstantReplacer> constantReplacer = Optional.ofNullable(view.getConstants())
        .map(c -> c.stream()
            .collect(toMap(ConstantDeclaration::getName, ConstantDeclaration::getValue)))
        .map(ConstantReplacer::new);

    // Build a new expression parser, and parse all the column expressions within the query.
    final ResourceType resourceType = ResourceType.fromCode(view.getResource());
    final ResourceCollection inputContext = ResourceCollection.build(fhirContext, dataSource,
        resourceType);
    final ParserContext parserContext = new ParserContext(inputContext, inputContext, fhirContext,
        sparkSession, dataSource, StaticFunctionRegistry.getInstance(), terminologyServiceFactory,
        constantReplacer);

    final List<Column> select = parseSelect(view.getSelect(), parserContext,
        Collections.emptyList());
    final List<String> where = view.getWhere() == null
                               ? Collections.emptyList()
                               : view.getWhere().stream()
                                   .map(WhereClause::getExpression)
                                   .collect(toList());
    final Optional<Column> filterCondition = where.stream()
        .map(e -> evalExpression(e, parserContext))
        .map(Collection::getColumn)
        .reduce(Column::and);

    final Dataset<Row> subjectDataset = dataSource.read(resourceType);

    return filterCondition
        .map(subjectDataset::filter)
        .orElse(subjectDataset)
        .select(select.toArray(new Column[0]));
  }

  @Nonnull
  private List<Column> parseSelect(@Nonnull final List<SelectClause> selectGroup,
      @Nonnull final ParserContext context, @Nonnull final List<Column> currentSelection) {
    final List<Column> newSelection = new ArrayList<>(currentSelection);

    for (final SelectClause select : selectGroup) {
      if (select instanceof DirectSelection) {
        newSelection.addAll(directSelection(context, (DirectSelection) select, currentSelection));

      } else if (select instanceof FromSelection) {
        newSelection.addAll(
            nestedSelection(context, (FromSelection) select, currentSelection, false));

      } else if (select instanceof ForEachSelection) {
        newSelection.addAll(
            nestedSelection(context, (ForEachSelection) select, currentSelection, true));

      } else {
        throw new IllegalStateException("Unknown select clause type: " + select.getClass());
      }
    }

    return newSelection;
  }

  @Nonnull
  private List<Column> directSelection(@Nonnull final ParserContext context,
      @Nonnull final DirectSelection select, @Nonnull final List<Column> currentSelection) {
    final Collection path = evalExpression(select.getPath(), context);
    final List<Column> newColumns = new ArrayList<>(currentSelection);
    newColumns.add(path.getColumn().alias(select.getAlias()));
    return newColumns;
  }

  @Nonnull
  @NotImplemented
  private List<Column> nestedSelection(final @Nonnull ParserContext context,
      @Nonnull final NestedSelectClause select, @Nonnull final List<Column> currentSelection,
      final boolean unnest) {
    final Collection from = evalExpression(select.getPath(), context);
    final Collection nextInputContext = unnest
                                        // TODO: FIX FIRST
                                        ? from
                                        // .unnest()
                                        : from;
    final ParserContext nextContext = context.withInputContext(nextInputContext);
    return parseSelect(select.getSelect(), nextContext, currentSelection);
  }

  @Nonnull
  private Collection evalExpression(@Nonnull final String expression,
      @Nonnull final ParserContext context) {
    final Parser parser = new Parser(context);
    final String updatedExpression = context.getConstantReplacer()
        .map(replacer -> replacer.execute(expression))
        .orElse(expression);
    return parser.evaluate(updatedExpression);
  }

}
