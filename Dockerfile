ARG REPO_DIR=/opt/graph-importer


# ===== stage 1 =====
FROM maven:3.9.6-eclipse-temurin-11-focal AS setup-env

ARG REPO_DIR

WORKDIR ${REPO_DIR}

COPY . .

SHELL ["/bin/bash", "-c"]

# run lint if container started
ENTRYPOINT []

CMD mvn -B -q checkstyle:check | \
    grep -i --color=never '\.java\|failed to execute goal' > lint.log && \
    exit 1 || \
    exit 0


# ===== stage 2 =====
FROM setup-env AS build-jar

RUN mvn clean package


# ===== stage 3 =====
FROM eclipse-temurin:11-jre-focal

ARG REPO_DIR

ARG JAR_FILE=target/GraphImporter-exec.jar

WORKDIR ${REPO_DIR}

COPY --from=build-jar ${REPO_DIR}/${JAR_FILE} ./target/
