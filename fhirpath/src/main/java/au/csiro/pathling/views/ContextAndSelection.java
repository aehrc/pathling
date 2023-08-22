package au.csiro.pathling.views;

import au.csiro.pathling.fhirpath.collection.Collection;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Column;

@Value
public class ContextAndSelection {

  @Nonnull
  Collection context;

  @Nonnull
  List<Column> selection;

  public void show() {
    context.getDataset().select(selection.toArray(new Column[0])).show(false);
  }

}
