# Path to the credentials file
CREDENTIALS_FILE="./secrets/cryptopulse-secret.json"

# Extract the project_id from the JSON credentials file
PROJECT_ID=$(jq -r '.project_id' "$CREDENTIALS_FILE")

# Create an .env file and write PROJECT_ID to it
echo "PROJECT_ID=$PROJECT_ID" > .env

# Run docker-compose
docker-compose up
