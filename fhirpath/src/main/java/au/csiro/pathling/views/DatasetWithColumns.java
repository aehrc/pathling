package au.csiro.pathling.views;

import java.util.List;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

@Value
public class DatasetWithColumns {

  @Nonnull
  Dataset<Row> dataset;
 
  @Nonnull
  List<Column> columns;

}
