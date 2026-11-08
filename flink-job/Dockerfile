FROM maven:3.9.9-eclipse-temurin-17 AS builder
WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn -B -DskipTests package

FROM flink:1.20.1-scala_2.12-java17
COPY --from=builder /build/target/flink-star-streaming-1.0.0.jar /opt/flink/usrlib/flink-star-streaming.jar
