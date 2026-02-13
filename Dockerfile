FROM eclipse-temurin:17-jre
WORKDIR /app

RUN mkdir -p /tmp/parquet-converter && \
    chmod 777 /tmp/parquet-converter

COPY target/*.jar app.jar
EXPOSE 8080

# Запуск
ENTRYPOINT ["java", "-jar", "app.jar"]