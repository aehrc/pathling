/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.test;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.assertj.core.api.ListAssert;
import org.assertj.core.api.ObjectAssert;

/**
 * @author Piotr Szul
 */
public class DatasetAssert {
  
  private final Dataset<Row> dataset;

  public DatasetAssert(Dataset<Row> dataset) {
    this.dataset = dataset;
  }

  public DatasetAssert isEmpty() {
    assertThat(dataset.isEmpty()).isTrue();
    return this;
  }

  public DatasetAssert hasRows(List<Row> expected) {
    assertThat(dataset.collectAsList()).isEqualTo(expected);
    return this;
  }

  public DatasetAssert hasRows(RowListBuilder expected) {
    return hasRows(expected.build());
  }

  public DatasetAssert hasRows(Row... expected) {
    return hasRows(Arrays.asList(expected));
  }

  public DatasetAssert debugSchema() {
    dataset.printSchema();
    return this;
  }

  public DatasetAssert debugRows() {
    dataset.show();
    return this;
  }

  public DatasetAssert debugAllRows() {
    dataset.collectAsList().forEach(System.out::println);
    return this;
  }


  public ObjectAssert<Object> isValue() {
    List<Row> result = dataset.collectAsList();
    assertThat(result.size()).isOne();
    assertThat(result.get(0).size()).isOne();
    return assertThat(result.get(0).get(0));
  }

  public ListAssert<Object> isValues() {
    List<Row> result = dataset.collectAsList();
    result.forEach(row -> assertThat(row.size()).isOne());
    return assertThat(result.stream().map(row -> row.get(0)).collect(Collectors.toList()));
  }
  
  public Dataset<Row> getDataset() {
    return dataset;
  }
}
