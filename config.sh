
#
# Authenticate with gcloud
#!/bin/bash

# Define environment variables
PROJECT_NAME="CryptoPulse"
PROJECT_ID="---- your-project-id ----"
SERVICE_ACCOUNT_NAME="cryptopulse"
BILLING_ACCOUNT_ID="--- your-billing-account-id"
KEY_PATH="./secrets/cryptooulse-secret.json"  # Path where the service account key will be saved

# Authenticate with gcloud (commented out if you want to run it manually)
# gcloud auth login

# Create a new project
gcloud projects create $PROJECT_ID --name=$PROJECT_NAME
echo "Project $PROJECT_NAME created."

# Link project to the billing account
gcloud alpha billing projects link $PROJECT_ID --billing-account=$BILLING_ACCOUNT_ID
echo "Project $PROJECT_ID linked to billing account $BILLING_ACCOUNT_ID."

# Set the project to be the current project
gcloud config set project $PROJECT_ID

# Create a service account
gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME \
    --description="Service account for BigQuery access" \
    --display-name="SentiStocks Service Account"
echo "Service account $SERVICE_ACCOUNT_NAME created."

# Bind BigQuery Administrator role to the service account
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"
echo "BigQuery Administrator role granted to $SERVICE_ACCOUNT_NAME."

# Create and download the service account key
gcloud iam service-accounts keys create $KEY_PATH \
    --iam-account "${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
echo "Service account key created and downloaded to $KEY_PATH."

# Activate the service account with gcloud
gcloud auth activate-service-account --key-file=$KEY_PATH
echo "Service account activated in gcloud configuration."

echo "Setup complete. You're now using your service account with gcloud."
