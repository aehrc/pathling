/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";
import { Button, Dialog, Intent, Label } from "@blueprintjs/core";

import "./style/SaveDialog.scss";
import { useState } from "react";

type Props = {
  isOpen: boolean;
  onSave: (name: string) => any;
  onClose: () => any;
};

function SaveDialog(props: Props) {
  const { isOpen, onClose, onSave } = props,
    [name, setName] = useState("");

  const handleChange = (event: any) => {
    setName(event.target.value);
  };

  return (
    <Dialog
      className="save-dialog"
      isOpen={isOpen}
      title="Save query"
      onClose={onClose}
    >
      <div className="save-dialog__body">
        <Label className="save-dialog__name-label">
          Name
          <input
            className="save-dialog__name-input"
            value={name}
            onChange={handleChange}
            autoFocus
          />
        </Label>
      </div>
      <div className="save-dialog__footer">
        <div className="save-dialog__footer-actions">
          <Button
            intent={Intent.PRIMARY}
            text="Save"
            onClick={() => onSave(name)}
          />
        </div>
      </div>
    </Dialog>
  );
}

export default SaveDialog;
