import React, { Component } from 'react'
import { connect } from 'react-redux'
import { Menu, Tag, Icon } from 'antd'

import * as actionCreators from '../../store/Actions'
import './DimensionList.less'

class DimensionList extends Component {
  handleSelectDimensionAttribute(dimensionAttribute) {
    const { selectDimensionAttribute, fetchQueryResult } = this.props
    selectDimensionAttribute(dimensionAttribute)
    fetchQueryResult()
  }

  componentDidMount() {
    const { fetchDimensions } = this.props
    return fetchDimensions()
  }

  render() {
    const { dimensions, loading } = this.props
    const iconType = loading ? 'loading' : 'appstore'
    return (
      <Menu mode="inline" defaultOpenKeys={['dimensions']}>
        <Menu.SubMenu
          key="dimensions"
          className="dimension-list"
          title={
            <span>
              <Icon
                className="dimension-list-icon"
                type={iconType}
                theme="filled"
              />
              <span>Dimensions</span>
            </span>
          }
        >
          {dimensions ? this.renderDimensions(dimensions) : null}
        </Menu.SubMenu>
      </Menu>
    )
  }

  renderDimensions(dimensions) {
    return dimensions
      .map((dimension, i) => {
        return (
          <Menu.ItemGroup
            className="dimension"
            title={
              <span>
                <Icon
                  className="dimension-icon"
                  type="appstore"
                  theme="outlined"
                />
                <span>{dimension.get('name')}</span>
              </span>
            }
            key={i}
          >
            <React.Fragment>
              <li className="dimension-attribute-list">
                {this.renderDimensionAttributes(dimension.get('attribute'))}
              </li>
            </React.Fragment>
          </Menu.ItemGroup>
        )
      })
      .toArray()
  }

  renderDimensionAttributes(dimensionAttributes) {
    return dimensionAttributes
      .map((reference, i) => {
        return (
          <Tag
            key={i}
            className="dimension-attribute"
            color="#038BE2"
            title={reference.get('display')}
            onClick={() => this.handleSelectDimensionAttribute(reference)}
          >
            <span>{reference.get('display')}</span>
            <Icon type="plus" />
          </Tag>
        )
      })
      .toArray()
  }
}

const mapStateToProps = state => ({
  dimensions: state.getIn(['dimensions', 'dimensions']),
  loading: state.getIn(['dimensions', 'loading']),
  selectedDimensions: state.getIn(['dimensions', 'selectedDimensions']),
})

export default connect(
  mapStateToProps,
  actionCreators,
)(DimensionList)
