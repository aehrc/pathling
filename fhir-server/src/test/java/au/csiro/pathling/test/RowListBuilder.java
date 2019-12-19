package au.csiro.pathling.test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;


/**
 * @author Piotr Szul
 */
public class RowListBuilder {
	private final Map<String, List<Object>> rowsAndValues = new TreeMap<String, List<Object>>();
	
	public RowListBuilder withRow(String id, Object value) {
		rowsAndValues.put(id, Arrays.asList(value));
		return this;
	}
	
	public List<Row> build() {
		return rowsAndValues.entrySet()
				.stream().flatMap(e -> e.getValue().stream().map(v -> RowFactory.create(e.getKey(), v))).collect(Collectors.toList());
	}
	
	public static RowListBuilder allWithValue(Object value, List<String> ids) {
		RowListBuilder empty = new RowListBuilder();
		ids.forEach(id -> empty.withRow(id, value));
		return empty;
	}
	
	public static RowListBuilder allNull(List<String> ids) {
		return allWithValue(null, ids);
	}
}