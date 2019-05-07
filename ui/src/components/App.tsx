/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";
import { connect } from "react-redux";
import { Position, Toaster } from "@blueprintjs/core";
import Resizable from "re-resizable";

import ElementTree from "./ElementTree";
import Aggregations from "./Aggregations";
import Filters from "./Filters";
import Groupings from "./Groupings";
import Actions from "./Actions";
import Result from "./Result";
import { GlobalState } from "../store";
import { Error } from "../store/ErrorReducer";
import * as actions from "../store/ConfigActions";
import "./style/App.scss";

const Alerter = Toaster.create({
  position: Position.BOTTOM_RIGHT
});

interface Props {
  error?: Error | null;
  fetchConfig: () => void;
}

interface State {
  siderWidth: number;
}

/**
 * Main application component.
 *
 * @author John Grimes
 */
class App extends React.Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { siderWidth: 300 };
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

  /**
   * Responds to the update of error details into global state by showing an
   * alert and logging the error to the console.
   */
  componentDidUpdate(prevProps: any): void {
    const { error } = this.props;
    if (error && prevProps.error !== error) {
      Alerter.show({ message: error.message, intent: "danger" });
      const opOutcome = error.opOutcome;
      // eslint-disable-next-line no-console
      if (opOutcome) console.error(opOutcome);
    }
  }

  handleResize(event: any): void {
    const { clientX: siderWidth } = event;
    if (siderWidth < 200) {
      this.setState(() => ({ siderWidth: 200 }));
    } else if (siderWidth > 600) {
      this.setState(() => ({ siderWidth: 600 }));
    } else {
      this.setState(() => ({ siderWidth }));
    }
  }

  render() {
    const { siderWidth } = this.state;

    return (
      <div
        className="app"
        style={{ gridTemplateColumns: `${siderWidth}px auto` }}
      >
        <div className="sider">
          <Resizable
            className="inner"
            enable={{
              top: false,
              right: true,
              bottom: false,
              left: false,
              topRight: false,
              bottomRight: false,
              bottomLeft: false,
              topLeft: false
            }}
            minWidth={200}
            maxWidth={600}
            onResize={this.handleResize}
          >
            <ElementTree />
          </Resizable>
        </div>
        <main className="content">
          <Aggregations />
          <Groupings />
          <Filters />
          <Actions />
          <Result />
        </main>
      </div>
    );
  }
}

const mapStateToProps = (state: GlobalState) => ({
  error: state.error
});

export default connect(
  mapStateToProps,
  actions
)(App);
