/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight.datasource;

import au.csiro.clinsight.persistence.Query;
import au.csiro.clinsight.persistence.QueryResult;
import org.hibernate.Session;
import org.hl7.fhir.dstu3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
public class HibernateQueryExecutor implements QueryExecutor {

    private static final Logger logger = LoggerFactory.getLogger(HibernateQueryExecutor.class);
    private static final String noValue = "(no value)";
    private Session session;
    private SqlQueryTranslator queryTranslator;

    @Autowired
    public HibernateQueryExecutor(Session session,
                                  SqlQueryTranslator queryTranslator) {
        this.session = session;
        this.queryTranslator = queryTranslator;
    }

    // TODO: Validate that all metrics are from the same fact, etc.
    private static void validateQuery(Query query) throws InvalidQueryException {
        List<Reference> metricRefs = query.getMetric();
        if (metricRefs == null) throw new InvalidQueryException("Query must refer to at least one Metric.");
    }

    private static QueryResult getQueryResult(Query query, ResultSet resultSet)
            throws SQLException, UnsupportedDataTypeException {
        QueryResult queryResult = new QueryResult();
        Reference queryReference = new Reference();
        queryReference.setResource(query);
        queryResult.setQuery(queryReference);

        int columnCount = resultSet.getMetaData().getColumnCount();
        int labelCount = query.getDimensionAttribute() == null ? 0 : query.getDimensionAttribute().size();
        int dataCount = query.getMetric().size();
        List<QueryResult.LabelComponent> labelList = processLabelHeadings(query, resultSet, columnCount, labelCount);
        List<QueryResult.DataComponent> dataList = processDataHeadings(query,
                                                                       resultSet,
                                                                       columnCount,
                                                                       labelCount,
                                                                       dataCount);
        while (resultSet.next()) {
            processLabels(resultSet, labelList, columnCount, labelCount);
            processData(resultSet, dataList, columnCount, labelCount, dataCount);
        }
        queryResult.setLabel(labelList);
        queryResult.setData(dataList);

        return queryResult;
    }

    private static List<QueryResult.LabelComponent> processLabelHeadings(Query query, ResultSet resultSet,
                                                                         int columnCount,
                                                                         int labelCount) throws SQLException {
        List<QueryResult.LabelComponent> labelList = new ArrayList<>();
        for (int i = 1; i <= labelCount && i <= columnCount; i++) {
            QueryResult.LabelComponent label = new QueryResult.LabelComponent();
            String seriesName = resultSet.getMetaData().getColumnLabel(i);
            label.setName(new StringType(seriesName));
            String reference = query.getDimensionAttribute().get(i - 1).getReference();
            Reference dimensionAttribute = new Reference();
            dimensionAttribute.setReference(reference);
            label.setDimensionAttribute(dimensionAttribute);
            label.setSeries(new ArrayList<>());
            labelList.add(label);
        }
        return labelList;
    }

    private static List<QueryResult.DataComponent> processDataHeadings(Query query, ResultSet resultSet,
                                                                       int columnCount,
                                                                       int labelCount, int dataCount)
            throws SQLException {
        List<QueryResult.DataComponent> dataList = new ArrayList<>();
        for (int i = labelCount + 1; i <= (labelCount + dataCount) && i <= columnCount; i++) {
            QueryResult.DataComponent data = new QueryResult.DataComponent();
            String seriesName = resultSet.getMetaData().getColumnLabel(i);
            data.setName(new StringType(seriesName));
            String reference = query.getMetric().get(i - labelCount - 1).getReference();
            Reference metric = new Reference();
            metric.setReference(reference);
            data.setMetric(metric);
            data.setSeries(new ArrayList<>());
            dataList.add(data);
        }
        return dataList;
    }

    private static void processLabels(ResultSet resultSet, List<QueryResult.LabelComponent> labelList,
                                      int columnCount,
                                      int labelCount) throws SQLException, UnsupportedDataTypeException {
        for (int i = 1; i <= labelCount && i <= columnCount; i++) {
            Type value = getValue(resultSet, i);
            labelList.get(i - 1).getSeries().add(value);
        }
    }

    private static void processData(ResultSet resultSet, List<QueryResult.DataComponent> dataList, int columnCount,
                                    int labelCount, int dataCount) throws SQLException, UnsupportedDataTypeException {
        for (int i = labelCount + 1; i <= (labelCount + dataCount) && i <= columnCount; i++) {
            Type value = getValue(resultSet, i);
            dataList.get(i - labelCount - 1).getSeries().add(value);
        }
    }

    private static Type getValue(ResultSet resultSet, int columnIndex)
            throws SQLException, UnsupportedDataTypeException {
        int columnType = resultSet.getMetaData().getColumnType(columnIndex);
        switch (columnType) {
            case Types.BIGINT:
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                return new IntegerType(resultSet.getInt(columnIndex));
            case Types.BOOLEAN:
                return new BooleanType(resultSet.getBoolean(columnIndex));
            case Types.CHAR:
            case Types.LONGNVARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
                String value = resultSet.getString(columnIndex);
                if (value == null) return new StringType(noValue);
                else return new StringType(value);
            case Types.DATE:
                return new DateType(resultSet.getDate(columnIndex));
            case Types.DECIMAL:
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.REAL:
                return new DecimalType(resultSet.getDouble(columnIndex));
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return new DateTimeType(resultSet.getTime(columnIndex));
            case Types.NULL:
                return new StringType(noValue);
            default:
                throw new UnsupportedDataTypeException(
                        "Unsupported data type encountered in result set: " + columnType);
        }
    }

    @Override
    public QueryResult execute(Query query) {
        return session.doReturningWork(connection -> {
            Statement statement = connection.createStatement();
            String sql = queryTranslator.translateQuery(query);
            long start = System.nanoTime();
            ResultSet resultSet = statement.executeQuery(sql);
            double elapsedMs = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
            logger.info("Executed query (" + String.format("%.1f", elapsedMs) + "ms): " + sql);
            return getQueryResult(query, resultSet);
        });
    }

    public static class UnsupportedDataTypeException extends RuntimeException {

        public UnsupportedDataTypeException(String message) {
            super(message);
        }

    }

}
