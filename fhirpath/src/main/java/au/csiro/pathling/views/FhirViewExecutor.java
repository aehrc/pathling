package au.csiro.pathling.views;

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.utilities.Lists;
import au.csiro.pathling.view.ColumnSelection;
import au.csiro.pathling.view.ExecutionContext;
import au.csiro.pathling.view.ExtractViewX;
import au.csiro.pathling.view.ForEachSelectionX;
import au.csiro.pathling.view.FromSelectionX;
import au.csiro.pathling.view.PrimitiveSelection;
import au.csiro.pathling.view.SelectionX;
import au.csiro.pathling.view.UnionAllSelection;
import ca.uhn.fhir.context.FhirContext;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
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

  public FhirViewExecutor(@Nonnull final FhirContext fhirContext,
      @Nonnull final SparkSession sparkSession, @Nonnull final DataSource dataset,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory) {
    this.fhirContext = fhirContext;
    this.sparkSession = sparkSession;
    this.dataSource = dataset;
  }

  @Nonnull
  public Dataset<Row> buildQuery(@Nonnull final FhirView view) {
    final ExecutionContext executionContext = new ExecutionContext(sparkSession, fhirContext,
        dataSource);
    final ExtractViewX extractView = toExtractView(view);
    return extractView.evaluate(executionContext);
  }


  @Nonnull
  private static List<SelectionX> toSelections(@Nonnull final List<SelectClause> select,
      @Nonnull final Parser parser) {
    return select.stream()
        .map(s -> toSelection(s, parser))
        .collect(Collectors.toUnmodifiableList());
  }

  @Nonnull
  private static List<SelectionX> selectionsFromSelectClause(
      @Nonnull final SelectClause selectClause,
      @Nonnull final Parser parser) {

    final Stream<SelectionX> columnSelection =
        !selectClause.getColumn().isEmpty()
        ? Stream.of(new ColumnSelection(
            selectClause.getColumn().stream()
                .map(c -> new PrimitiveSelection(parser.parse(c.getPath()),
                    Optional.ofNullable(c.getName()), c.isCollection()))
                .collect(Collectors.toUnmodifiableList())))
        : Stream.empty();

    final Stream<SelectionX> unionAll =
        !selectClause.getUnionAll().isEmpty()
        ? Stream.of(new UnionAllSelection(toSelections(selectClause.getUnionAll(), parser)))
        : Stream.empty();

    return Stream.of(
            columnSelection,
            selectClause.getSelect().stream().map(s -> toSelection(s, parser)),
            unionAll
        )
        .flatMap(Function.identity())
        .collect(Collectors.toUnmodifiableList());
  }

  @Nonnull
  private static SelectionX toSelection(@Nonnull final SelectClause select,
      @Nonnull final Parser parser) {
    // TODO: remove the classes for each select type and just use SelectClause
    if (select instanceof FromSelect) {
      return new FromSelectionX(
          selectionsFromSelectClause(select, parser));
    } else if (select instanceof ForEachSelect) {
      return new ForEachSelectionX(parser.parse(requireNonNull(select.getPath())),
          selectionsFromSelectClause(select, parser), false);
    } else if (select instanceof ForEachOrNullSelect) {
      return new ForEachSelectionX(parser.parse(requireNonNull(select.getPath())),
          selectionsFromSelectClause(select, parser), true);
    } else {
      throw new IllegalStateException("Unknown select clause type: " + select.getClass());
    }
  }

  @Nonnull
  static ExtractViewX toExtractView(@Nonnull final FhirView fhirView) {
    final Parser parser = new Parser();
    final SelectionX selection = parseSelection(fhirView, parser);
    final Optional<SelectionX> where = parseWhere(fhirView, parser);
    return new ExtractViewX(ResourceType.fromCode(fhirView.getResource()),
        selection, where);
  }

  @Nonnull
  private static SelectionX parseSelection(@Nonnull FhirView fhirView, Parser parser) {
    final List<SelectionX> selectionComponents = toSelections(fhirView.getSelect(), parser);
    return new FromSelectionX(selectionComponents);
  }

  @Nonnull
  private static Optional<SelectionX> parseWhere(@Nonnull FhirView fhirView, Parser parser) {
    final List<PrimitiveSelection> whereComponents = Optional.ofNullable(fhirView.getWhere())
        .stream().flatMap(List::stream)
        .map(WhereClause::getExpression)
        .map(parser::parse)
        .map(fp -> new PrimitiveSelection(fp))
        .collect(Collectors.toUnmodifiableList());

    final Optional<SelectionX> whereSelection =
        Lists.optionalOf(whereComponents).map(ColumnSelection::new);
    return whereSelection;
  }

}
