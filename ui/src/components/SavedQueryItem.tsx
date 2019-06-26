/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React, { MouseEvent } from "react";
import { Button, ButtonGroup } from "@blueprintjs/core";
import { SavedQuery, SavedQueryWithStatus } from "../store/SavedQueriesReducer";

interface Props {
  query: SavedQueryWithStatus;
  loaded: boolean;
  onClick?: (query: SavedQuery) => any;
  onClickEdit?: (query: SavedQuery) => any;
  onClickDelete?: (query: SavedQuery) => any;
}

function SavedQueryItem(props: Props) {
  const { query, loaded, onClick, onClickEdit, onClickDelete } = props;

  const handleClickEdit = (event: MouseEvent) => {
    event.stopPropagation();
    onClickEdit(query);
  };

  const handleClickDelete = (event: MouseEvent) => {
    event.stopPropagation();
    onClickDelete(query);
  };

  return (
    <li
      className={
        loaded
          ? "saved-queries__query saved-queries__query--loaded"
          : "saved-queries__query"
      }
      onClick={loaded ? null : () => onClick(query)}
      title="Load this query"
    >
      <ButtonGroup className="saved-queries__query-actions" minimal>
        <Button
          icon="edit"
          title="Edit query name"
          small
          onClick={handleClickEdit}
        />
        <Button
          icon="trash"
          title="Delete query"
          small
          onClick={handleClickDelete}
        />
      </ButtonGroup>
      <div className="saved-queries__query-name">{query.name}</div>
    </li>
  );
}

export default SavedQueryItem;
