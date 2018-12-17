import React, { Component } from 'react'
import { connect } from 'react-redux'
import { Icon, Tag } from 'antd'

import * as actionCreators from '../../store/Actions'
import './SelectedDimensionAttributes.less'

class SelectedDimensionAttributes extends Component {
  handleDeselectDimensionAttribute(dimensionAttribute) {
    const { deselectDimensionAttribute, fetchQueryResult } = this.props
    deselectDimensionAttribute(dimensionAttribute)
    fetchQueryResult()
  }

  render() {
    const { selectedDimensionAttributes } = this.props
    return (
      <div className="selected-dimension-attributes">
        {selectedDimensionAttributes.size > 0 ? (
          this.renderDimensionAttributes(selectedDimensionAttributes)
        ) : (
          <React.Fragment>
            <Icon type="appstore" />
            <span>Dimension Attributes</span>
          </React.Fragment>
        )}
      </div>
    )
  }

  renderDimensionAttributes(dimensionAttributes) {
    return (
      <div className="dimension-attributes">
        {dimensionAttributes.map((dimensionAttribute, i) => {
          return (
            <Tag
              key={i}
              className="dimension-attribute"
              color="#038BE2"
              title={dimensionAttribute.get('display')}
              onClick={() =>
                this.handleDeselectDimensionAttribute(dimensionAttribute)
              }
            >
              <span>{dimensionAttribute.get('display')}</span>
              <Icon type="close" />
            </Tag>
          )
        })}
      </div>
    )
  }
}

const mapStateToProps = state => ({
  selectedDimensionAttributes: state.getIn([
    'dimensions',
    'selectedDimensionAttributes',
  ]),
})

export default connect(
  mapStateToProps,
  actionCreators,
)(SelectedDimensionAttributes)
