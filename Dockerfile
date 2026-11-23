FROM maven:3.9.8-eclipse-temurin-21-alpine AS builder

WORKDIR /build

COPY pom.xml .
RUN mvn dependency:go-offline -B

COPY src ./src
RUN mvn clean package -DskipTests -B

FROM eclipse-temurin:21-jre-alpine AS runtime

ENV APPLICATION_USER=appuser \
    APPLICATION_GROUP=appgroup \
    APP_HOME=/app \
    JAVA_OPTS="-XX:+UseZGC -XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError"

RUN addgroup -g 10001 -S ${APPLICATION_GROUP} && \
    adduser -u 10001 -S ${APPLICATION_USER} -G ${APPLICATION_GROUP} -s /sbin/nologin -h ${APP_HOME}

WORKDIR ${APP_HOME}

COPY --from=builder --chown=10001:10001 /build/target/*.jar app.jar

USER 10001:10001

EXPOSE 8080

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
