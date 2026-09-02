# === ЭТАП 1: Сборка ===
FROM maven:3.9.6-eclipse-temurin-21 AS builder
WORKDIR /app
COPY . .

# Собираем проект. Maven сам найдет запускаемый модуль,
# выполнит repackage и создаст исполняемый JAR.
RUN mvn clean install -DskipTests -B

# === ЭТАП 2: Запуск ===
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

# Берем файл напрямую из папки target модуля app, а не из корня /app/app.jar!
COPY --from=builder /app/app/target/app-*.jar /app/app.jar

ENTRYPOINT ["java", "-jar", "/app/app.jar"]