/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { Button, ButtonGroup } from "@blueprintjs/core";
import React, { ChangeEvent, MouseEvent, KeyboardEvent, useState } from "react";
import { SavedQuery, SavedQueryWithStatus } from "../store/SavedQueriesReducer";
import "./style/EditableQueryItem.scss";

interface Props {
  query: SavedQueryWithStatus;
  onClickAccept?: (query: SavedQuery) => any;
  onClickCancel?: (query: SavedQuery) => any;
}

function EditableQueryItem(props: Props) {
  const { query, onClickAccept, onClickCancel } = props,
    [updatedQuery, updateQuery] = useState(query);

  const handleChange = (event: ChangeEvent<HTMLTextAreaElement>) => {
    updateQuery({ ...updatedQuery, name: event.target.value });
  };

  const handleKeyDown = (event: KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key === "Enter") onClickAccept(updatedQuery);
  };

  const handleClickAccept = (event: MouseEvent) => {
    event.stopPropagation();
    if (onClickAccept) onClickAccept(updatedQuery);
  };

  const handleClickCancel = (event: MouseEvent) => {
    event.stopPropagation();
    if (onClickCancel) onClickCancel(updatedQuery);
  };

  return (
    <li className="editable-query-item">
      <textarea
        className="editable-query-item__input"
        onChange={handleChange}
        onKeyDown={handleKeyDown}
        value={updatedQuery.name}
      />
      <ButtonGroup className="editable-query-item__actions" minimal>
        <Button
          className="editable-query-item__actions__accept"
          icon="tick"
          title="Accept changes"
          small
          onClick={handleClickAccept}
        />
        <Button
          className="editable-query-item__actions__cancel"
          icon="cross"
          title="Cancel changes"
          small
          onClick={handleClickCancel}
        />
      </ButtonGroup>
    </li>
  );
}

export default EditableQueryItem;
