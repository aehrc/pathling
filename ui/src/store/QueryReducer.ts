/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { QueryAction } from "./QueryActions";

export interface Query {
  aggregations: Aggregation[];
  groupings: Grouping[];
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

const initialState: Query = {
  aggregations: [],
  groupings: []
};

export default (state = initialState, action: QueryAction): Query => {
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
    case "CLEAR_QUERY":
      return initialState;
    default:
      return state;
  }
};
