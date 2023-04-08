#! /bin/bash
# Empty the azure storage bucket after testing

set +a
source .env
set -a

# Retrieve the storage account key
AZURE_STORAGE_ACCOUNT_KEY=$(az storage account keys list --account-name $AZURE_STORAGE_ACCOUNT --query '[0].value' -o tsv)

# List all the blobs in the container
BLOBS=$(az storage blob list --account-name $AZURE_STORAGE_ACCOUNT --account-key "$AZURE_STORAGE_ACCOUNT_KEY" --container-name $BLOB_CONTAINER --query '[].name' -o tsv)

# Delete each blob
for BLOB in $BLOBS; do
    echo "Deleting blob $BLOB"
    az storage blob delete --account-name $AZURE_STORAGE_ACCOUNT --account-key "$AZURE_STORAGE_ACCOUNT_KEY" --container-name $BLOB_CONTAINER --name "$BLOB"
done

echo "All blobs have been deleted"
