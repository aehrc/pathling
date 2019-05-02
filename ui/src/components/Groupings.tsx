/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";
import { connect } from "react-redux";
import { Icon, Tag } from "@blueprintjs/core";

import * as actions from "../store/QueryActions";
import { Grouping } from "../store/QueryReducer";
import "./style/Groupings.scss";
import { GlobalState } from "../store";
import { ReactElement } from "react";

interface Props {
  groupings: Grouping[];
  removeGrouping: (index: number) => void;
}

/**
 * Renders a list of currently selected groupings, used when composing a query.
 *
 * @author John Grimes
 */
function Groupings(props: Props) {
  const { groupings, removeGrouping } = props;

  const handleRemove = (index: number): void => {
    removeGrouping(index);
  };

  const renderBlankCanvas = (): ReactElement => (
    <div className="blank-canvas">Groupings</div>
  );

  const renderGroupings = (): ReactElement[] =>
    groupings.map((grouping, i) => (
      <Tag key={i} round={true} large={true} onRemove={() => handleRemove(i)}>
        {grouping.label}
      </Tag>
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
