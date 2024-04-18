
docker build \
    -t mage \
    -f ./mage.dockerfile .

docker build \
    -t streamlit-dashboard\
    -f ./dashboard.Dockerfile .
