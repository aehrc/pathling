package au.csiro.pathling.views;

import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.path.Paths.ExtConsFhir;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.query.QueryParser;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.view.ExecutionContext;
import au.csiro.pathling.view.ExtractView;
import au.csiro.pathling.view.ForEachOrNullSelection;
import au.csiro.pathling.view.ForEachSelection;
import au.csiro.pathling.view.FromSelection;
import au.csiro.pathling.view.PrimitiveSelection;
import au.csiro.pathling.view.Selection;
import ca.uhn.fhir.context.FhirContext;
import java.util.List;
import java.util.Optional;
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
    final ExtractView extractView = toExtractView(view);
    return extractView.evaluate(executionContext);
  }


  @Nonnull
  private static List<Selection> toSelections(@Nonnull final List<SelectClause> select,
      @Nonnull final Parser parser) {
    return select.stream()
        .map(s -> toSelection(s, parser))
        .collect(Collectors.toUnmodifiableList());
  }

  @Nonnull
  private static List<Selection> selectionsFromSelectClause(
      @Nonnull final SelectClause selectClause,
      @Nonnull final Parser parser) {
    return Stream.concat(
            selectClause.getColumn().stream()
                .map(c -> new PrimitiveSelection(parser.parse(c.getPath()),
                    Optional.ofNullable(c.getName()), c.isCollection())),
            selectClause.getSelect().stream()
                .map(s -> toSelection(s, parser))
        )
        .collect(Collectors.toUnmodifiableList());
  }

  @Nonnull
  private static Selection toSelection(@Nonnull final SelectClause select,
      @Nonnull final Parser parser) {
    // TODO: move to the classes ???
    if (select instanceof FromSelect) {
      return new FromSelection(nonNull(select.getPath())
                               ? parser.parse(requireNonNull(select.getPath()))
                               : FhirPath.nullPath(),
          selectionsFromSelectClause(select, parser));
    } else if (select instanceof ForEachSelect) {
      return new ForEachSelection(parser.parse(requireNonNull(select.getPath())),
          selectionsFromSelectClause(select, parser));
    } else if (select instanceof ForEachOrNullSelect) {
      return new ForEachOrNullSelection(parser.parse(requireNonNull(select.getPath())),
          selectionsFromSelectClause(select, parser));
    } else {
      throw new IllegalStateException("Unknown select clause type: " + select.getClass());
    }
  }

  @Nonnull
  static ExtractView toExtractView(@Nonnull final FhirView fhirView) {

    final Parser parser = new Parser();

    final List<Selection> selectionComponents = toSelections(fhirView.getSelect(), parser);

    final Selection selection = new FromSelection(new ExtConsFhir("%resource"),
        selectionComponents);

    final Optional<Selection> whereSelection = QueryParser.decomposeFilter(
        Optional.ofNullable(fhirView.getWhere())
            .stream().flatMap(List::stream)
            .map(WhereClause::getExpression)
            .map(parser::parse)
            .collect(Collectors.toUnmodifiableList()));

    return new ExtractView(ResourceType.fromCode(fhirView.getResource()),
        selection, whereSelection);
  }

}