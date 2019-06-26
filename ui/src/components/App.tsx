/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { FocusStyleManager } from "@blueprintjs/core";
import Resizable from "re-resizable";
import * as React from "react";
import { connect } from "react-redux";
import { GlobalState } from "../store";

import ElementTree from "./ElementTree";
import Filters from "./Filters";
import Groupings from "./Groupings";
import Result from "./Result";
import SavedQueries from "./SavedQueries";
import * as actions from "../store/ConfigActions";
import Actions from "./Actions";
import Aggregations from "./Aggregations";
import Alerter from "./Alerter";
import "./style/App.scss";

interface Props {
  queryName?: string;
  fetchConfig: () => void;
}

interface State {
  leftSiderWidth: number;
  rightSiderWidth: number;
}

// Causes focus styles to be hidden while the user interacts using the mouse
// and to appear when the tab key is pressed to begin keyboard navigation.
//
// See https://blueprintjs.com/docs/#core/accessibility
FocusStyleManager.onlyShowFocusOnTabs();

/**
 * Main application component.
 *
 * @author John Grimes
 */
class App extends React.Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { leftSiderWidth: 300, rightSiderWidth: 300 };
    this.handleResize = this.handleResize.bind(this);
  }

  /**
   * Fetches the application configuration file.
   */
  componentDidMount(): void {
    const { fetchConfig } = this.props;
    fetchConfig();
  }

  /**
   * Catches any uncaught errors that are thrown during the rendering of
   * components.
   */
  componentDidCatch(error: Error): void {
    Alerter.show({ message: error.message, intent: "danger" });
    // eslint-disable-next-line no-console
    console.error(error);
  }

  handleResize(event: any, direction: any): void {
    const siderWidthKey =
        direction === "left" ? "leftSiderWidth" : "rightSiderWidth",
      siderWidth =
        direction === "left"
          ? event.clientX
          : window.innerWidth - event.clientX,
      minSiderWidth = 0,
      maxSiderWidth = 999999;
    if (siderWidth < minSiderWidth) {
      this.setState(() => ({ ...this.state, [siderWidthKey]: minSiderWidth }));
    } else if (siderWidth > maxSiderWidth) {
      this.setState(() => ({ ...this.state, [siderWidthKey]: maxSiderWidth }));
    } else {
      this.setState(() => ({ ...this.state, [siderWidthKey]: siderWidth }));
    }
  }

  render() {
    const { queryName } = this.props,
      { leftSiderWidth, rightSiderWidth } = this.state;

    return (
      <div className="app">
        <div
          className="app__left-sider"
          style={{ flexBasis: `${leftSiderWidth}px` }}
        >
          <h2>Data elements</h2>
          <ElementTree />
        </div>
        <main className="app__content">
          <Resizable
            className="app__content-inner"
            enable={{
              top: false,
              right: true,
              bottom: false,
              left: true,
              topRight: false,
              bottomRight: false,
              bottomLeft: false,
              topLeft: false
            }}
            handleWrapperClass="app__content-handle"
            handleStyles={{
              right: { width: "1em", right: "-0.5em" },
              left: { width: "1em", left: "-0.5em" }
            }}
            onResize={this.handleResize}
          >
            <h2>
              <span className="app__content-title">Query</span>
              {queryName ? ` \u2014 \u201c${queryName}\u201d` : null}
            </h2>
            <div className="app__query">
              <Aggregations />
              <Groupings />
              <Filters />
            </div>
            <Actions />
            <Result />
          </Resizable>
        </main>
        <div
          className="app__right-sider"
          style={{ flexBasis: `${rightSiderWidth}px` }}
        >
          <h2>Saved queries</h2>
          <SavedQueries />
        </div>
      </div>
    );
  }
}

const mapStateToProps = (state: GlobalState) => ({
  queryName: state.query.name
});

export default connect(
  mapStateToProps,
  actions
)(App);
