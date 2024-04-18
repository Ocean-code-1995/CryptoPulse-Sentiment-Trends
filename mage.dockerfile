# Description: Dockerfile for the Mage AI container
FROM mageai/mageai:latest

# Set the working directory
COPY ./mage /home/src/mage

# Add Debian Bullseye repository
RUN echo 'deb http://deb.debian.org/debian bullseye main' > /etc/apt/sources.list.d/bullseye.list

# Install OpenJDK 11
RUN apt-get update -y && \
    apt-get install -y openjdk-11-jdk

# download and install spark


# Remove Debian Bullseye repository
RUN rm /etc/apt/sources.list.d/bullseye.list

# Install Python requirements
RUN pip install pyspark
RUN pip install python-binance
RUN pip install praw

# Set environment variables
ENV MAGE_DATA_DIR=/home/src/

# entry point for the mage container to start the cryptosentiment module
ENTRYPOINT [ "mage", "start", "/home/src/mage/cryptosentiment"]