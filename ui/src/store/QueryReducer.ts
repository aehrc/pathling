/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { QueryAction } from "./QueryActions";

export interface QueryState {
  aggregations: Aggregation[];
  groupings: Grouping[];
  filters: Filter[];
}

export interface QueryStateWithName extends QueryState {
  name?: string;
}

export interface Aggregation {
  label?: string;
  expression: string;
}

export interface PartialAggregation {
  label?: string;
  expression?: string;
}

export interface Grouping {
  label?: string;
  expression: string;
}

export interface PartialGrouping {
  label?: string;
  expression?: string;
}

export interface Filter {
  label?: string;
  expression: string;
}

export interface PartialFilter {
  label?: string;
  expression?: string;
}

const initialState: QueryStateWithName = {
  aggregations: [],
  groupings: [],
  filters: []
};

export default (
  state = initialState,
  action: QueryAction
): QueryStateWithName => {
  switch (action.type) {
    case "ADD_AGGREGATION":
      return {
        ...state,
        aggregations: state.aggregations.concat({
          expression: action.expression,
          label: action.expression
        })
      };
    case "REMOVE_AGGREGATION":
      return {
        ...state,
        aggregations: state.aggregations.filter((_, i) => i !== action.index)
      };
    case "UPDATE_AGGREGATION":
      return {
        ...state,
        aggregations: state.aggregations.map((aggregation, i) =>
          i === action.index
            ? { ...aggregation, ...action.aggregation }
            : aggregation
        )
      };
    case "ADD_GROUPING":
      return {
        ...state,
        groupings: state.groupings.concat({
          expression: action.expression,
          label: action.expression
        })
      };
    case "REMOVE_GROUPING":
      return {
        ...state,
        groupings: state.groupings.filter((_, i) => i !== action.index)
      };
    case "UPDATE_GROUPING":
      return {
        ...state,
        groupings: state.groupings.map((grouping, i) =>
          i === action.index ? { ...grouping, ...action.grouping } : grouping
        )
      };
    case "ADD_FILTER":
      return {
        ...state,
        filters: state.filters.concat({
          expression: action.expression,
          label: action.expression
        })
      };
    case "REMOVE_FILTER":
      return {
        ...state,
        filters: state.filters.filter((_, i) => i !== action.index)
      };
    case "UPDATE_FILTER":
      return {
        ...state,
        filters: state.filters.map((filter, i) =>
          i === action.index ? { ...filter, ...action.filter } : filter
        )
      };
    case "CLEAR_QUERY":
      return initialState;
    case "LOAD_QUERY":
      return {
        ...state,
        ...action.query,
        name: action.name
      };
    default:
      return state;
  }
};
