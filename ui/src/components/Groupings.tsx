/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";
import { connect } from "react-redux";
import { Icon, Tag } from "@blueprintjs/core";

import * as actions from "../store/QueryActions";
import { Grouping, PartialGrouping } from "../store/QueryReducer";
import { GlobalState } from "../store";
import { ReactElement } from "react";
import "./style/Groupings.scss";
import ExpressionEditor from "./ExpressionEditor";

interface Props {
  groupings: Grouping[];
  removeGrouping: (index: number) => void;
  updateGrouping: (index: number, grouping: PartialGrouping) => void;
}

/**
 * Renders a list of currently selected groupings, used when composing a query.
 *
 * @author John Grimes
 */
function Groupings(props: Props) {
  const { groupings, removeGrouping, updateGrouping } = props;

  const handleRemove = (index: number): void => {
    removeGrouping(index);
  };

  const handleChange = (i: number, grouping: PartialGrouping) => {
    updateGrouping(i, grouping);
  };

  const renderBlankCanvas = (): ReactElement => (
    <div className="blank-canvas">Groupings</div>
  );

  const renderGroupings = (): ReactElement[] =>
    groupings.map((grouping, i) => (
      <ExpressionEditor
        key={i}
        expression={grouping}
        onChange={grouping => handleChange(i, grouping)}
      >
        <Tag round={true} large={true} onRemove={() => handleRemove(i)}>
          {grouping.label}
        </Tag>
      </ExpressionEditor>
    ));

  return (
    <div className="groupings">
      <Icon className="section-identity" icon="graph" />
      {groupings.length === 0 ? renderBlankCanvas() : renderGroupings()}
    </div>
  );
}

const mapStateToProps = (state: GlobalState) => ({
  groupings: state.query.groupings
});
export default connect(
  mapStateToProps,
  actions
)(Groupings);
