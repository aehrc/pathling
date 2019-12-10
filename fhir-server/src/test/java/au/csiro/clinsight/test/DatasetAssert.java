package au.csiro.clinsight.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.assertj.core.api.ObjectAssert;


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
	
	
	public ObjectAssert<Object> isValue() {
		List<Row> result = dataset.collectAsList();
		assertThat(result.size()).isOne();
		assertThat(result.get(0).size()).isOne();
		return assertThat(result.get(0).get(0));
	}
}
