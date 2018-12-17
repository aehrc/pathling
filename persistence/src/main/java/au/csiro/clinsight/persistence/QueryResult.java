/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight.persistence;

import ca.uhn.fhir.model.api.annotation.Block;
import ca.uhn.fhir.model.api.annotation.Child;
import ca.uhn.fhir.model.api.annotation.ResourceDef;
import ca.uhn.fhir.util.ElementUtil;
import org.hl7.fhir.dstu3.model.*;

import java.util.List;
import java.util.stream.Collectors;

import static ca.uhn.fhir.model.api.annotation.Child.MAX_UNLIMITED;

/**
 * Describes the response to a request for aggregate statistics about data held within a FHIR analytics server.
 *
 * @author John Grimes
 */
@ResourceDef(name = "QueryResult",
             profile = "https://clinsight.csiro.au/fhir/StructureDefinition/query-result-0")
public class QueryResult extends DomainResource {

    @Child(name = "query", min = 1, order = 0, summary = true)
    private Reference query;

    @Child(name = "label", max = MAX_UNLIMITED, order = 1)
    private List<LabelComponent> label;

    @Child(name = "data", max = MAX_UNLIMITED, order = 2)
    private List<DataComponent> data;

    public Reference getQuery() {
        return query;
    }

    public void setQuery(Reference query) {
        this.query = query;
    }

    public List<LabelComponent> getLabel() {
        return label;
    }

    public void setLabel(List<LabelComponent> label) {
        this.label = label;
    }

    public List<DataComponent> getData() {
        return data;
    }

    public void setData(List<DataComponent> data) {
        this.data = data;
    }

    @Override
    public DomainResource copy() {
        QueryResult queryResult = new QueryResult();
        queryResult.query = query;
        queryResult.label = label;
        queryResult.data = data;
        return queryResult;
    }

    @Override
    public void copyValues(DomainResource dst) {
        super.copyValues(dst);
        if (query != null) {
            ((QueryResult) dst).query = query.copy();
        }
        if (label != null) {
            ((QueryResult) dst).label = label.stream().map(LabelComponent::copy).collect(Collectors.toList());
        }
        if (data != null) {
            ((QueryResult) dst).data = data.stream().map(DataComponent::copy).collect(Collectors.toList());
        }
    }

    @Override
    public ResourceType getResourceType() {
        return null;
    }

    @Override
    public boolean isEmpty() {
        return super.isEmpty() && ElementUtil.isEmpty(query, label, data);
    }

    @Block
    public static class LabelComponent extends BackboneElement {

        @Child(name = "name", order = 0)
        private StringType name;

        @Child(name = "dimensionAttribute", min = 1, order = 1)
        private Reference dimensionAttribute;

        @Child(name = "series", max = MAX_UNLIMITED, order = 2)
        private List<Type> series;

        public StringType getName() {
            return name;
        }

        public void setName(StringType name) {
            this.name = name;
        }

        public Reference getDimensionAttribute() {
            return dimensionAttribute;
        }

        public void setDimensionAttribute(Reference dimensionAttribute) {
            this.dimensionAttribute = dimensionAttribute;
        }

        public List<Type> getSeries() {
            return series;
        }

        public void setSeries(List<Type> series) {
            this.series = series;
        }

        @Override
        public LabelComponent copy() {
            LabelComponent labelComponent = new LabelComponent();
            copyValues(labelComponent);
            return labelComponent;
        }

        @Override
        public void copyValues(BackboneElement dst) {
            super.copyValues(dst);
            if (name != null) {
                ((LabelComponent) dst).name = name.copy();
            }
            if (dimensionAttribute != null) {
                ((LabelComponent) dst).dimensionAttribute = dimensionAttribute.copy();
            }
            if (series != null) {
                ((LabelComponent) dst).series = series.stream().map(Type::copy).collect(Collectors.toList());
            }
        }

        @Override
        public boolean isEmpty() {
            return super.isEmpty() && ElementUtil.isEmpty(name, dimensionAttribute, series);
        }

    }

    @Block
    public static class DataComponent extends BackboneElement {

        @Child(name = "name", order = 0)
        private StringType name;

        @Child(name = "metric", min = 1, order = 1)
        private Reference metric;

        @Child(name = "series", max = MAX_UNLIMITED, order = 2)
        private List<Type> series;

        public StringType getName() {
            return name;
        }

        public void setName(StringType name) {
            this.name = name;
        }

        public Reference getMetric() {
            return metric;
        }

        public void setMetric(Reference metric) {
            this.metric = metric;
        }

        public List<Type> getSeries() {
            return series;
        }

        public void setSeries(List<Type> series) {
            this.series = series;
        }

        @Override
        public DataComponent copy() {
            DataComponent dataComponent = new DataComponent();
            copyValues(dataComponent);
            return dataComponent;
        }

        @Override
        public void copyValues(BackboneElement dst) {
            super.copyValues(dst);
            if (name != null) {
                ((DataComponent) dst).name = name.copy();
            }
            if (metric != null) {
                ((DataComponent) dst).metric = metric.copy();
            }
            if (series != null) {
                ((DataComponent) dst).series = series.stream().map(Type::copy).collect(Collectors.toList());
            }
        }

        @Override
        public boolean isEmpty() {
            return super.isEmpty() && ElementUtil.isEmpty(name, metric, series);
        }

    }

}
