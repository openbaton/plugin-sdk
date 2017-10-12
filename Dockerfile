FROM openjdk:8-jdk
COPY . /project
WORKDIR /project
RUN ./gradlew install
