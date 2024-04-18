
# Assuming the JSON credentials are stored in 'credentials.json'
CREDENTIALS_FILE="./secrets/cryptopulse-secret.json"

# Use jq to parse JSON and extract project_id
PROJECT_ID=$(jq -r '.project_id' $CREDENTIALS_FILE)

# Export the PROJECT_ID so it's available to docker-compose as an environment variable
export PROJECT_ID

# Now run docker-compose
docker-compose up