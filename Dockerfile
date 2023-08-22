FROM maven:3.8.6-openjdk-11-slim

# creating temporary working directory in order to store files
WORKDIR /app

# RUN apt-get update 

#Install required packages and dependencies
RUN apt-get install -y mysql-client

#Copy the project files into the container
COPY . /app

#Running the commands that existed in the graph-importer Jenkinsfile
#Builds the jar file using Maven
RUN mvn clean package -DskipTests

# Define the entry point script
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh
ENTRYPOINT ["/app/entrypoint.sh", "--help"]
