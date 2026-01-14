/**
 * Dialog for confirming deletion of a FHIR resource.
 *
 * @author John Grimes
 */

import { TrashIcon } from "@radix-ui/react-icons";
import { AlertDialog, Button, Flex, Spinner, Text } from "@radix-ui/themes";

interface DeleteConfirmationDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  resourceType: string;
  resourceId: string;
  resourceSummary: string | null;
  onConfirm: () => void;
  isDeleting: boolean;
}

/**
 * Confirmation dialog for deleting a FHIR resource.
 *
 * @param root0 - The component props.
 * @param root0.open - Whether the dialog is open.
 * @param root0.onOpenChange - Callback when dialog open state changes.
 * @param root0.resourceType - Type of the resource to delete.
 * @param root0.resourceId - ID of the resource to delete.
 * @param root0.resourceSummary - Human-readable summary of the resource.
 * @param root0.onConfirm - Callback when deletion is confirmed.
 * @param root0.isDeleting - Whether deletion is in progress.
 * @returns The delete confirmation dialog component.
 */
export function DeleteConfirmationDialog({
  open,
  onOpenChange,
  resourceType,
  resourceId,
  resourceSummary,
  onConfirm,
  isDeleting,
}: DeleteConfirmationDialogProps) {
  const handleCancel = () => {
    onOpenChange(false);
  };

  return (
    <AlertDialog.Root open={open} onOpenChange={onOpenChange}>
      <AlertDialog.Content maxWidth="450px">
        <AlertDialog.Title>Delete resource</AlertDialog.Title>
        <AlertDialog.Description size="2">
          <Text as="p">
            Are you sure you want to delete this resource? This action cannot be undone.
          </Text>
          <Text as="p" mt="2" weight="medium">
            {resourceType}/{resourceId}
          </Text>
          {resourceSummary && (
            <Text as="p" color="gray">
              {resourceSummary}
            </Text>
          )}
        </AlertDialog.Description>
        <Flex gap="3" mt="4" justify="end">
          <AlertDialog.Cancel>
            <Button variant="soft" color="gray" onClick={handleCancel} disabled={isDeleting}>
              Cancel
            </Button>
          </AlertDialog.Cancel>
          <AlertDialog.Action>
            <Button color="red" onClick={onConfirm} disabled={isDeleting}>
              {isDeleting ? <Spinner /> : <TrashIcon />}
              Delete
            </Button>
          </AlertDialog.Action>
        </Flex>
      </AlertDialog.Content>
    </AlertDialog.Root>
  );
}
