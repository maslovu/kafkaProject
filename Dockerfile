# === ЭТАП 1: Сборка ===
FROM maven:3.9.6-eclipse-temurin-21 AS builder
WORKDIR /app
COPY . .

# Собираем проект. Maven сам найдет запускаемый модуль,
# выполнит repackage и создаст исполняемый JAR.
RUN mvn clean package -DskipTests -B

# ХИТРОСТЬ: Находим созданный исполняемый JAR-файл в любом месте проекта
# и копируем его в фиксированное место в корне сборщика под именем app.jar.
# Это избавит нас от жестко прописанных путей вида /app/consumer/target/...
RUN cp /app/app/target/*.jar /app/app.jar

# === ЭТАП 2: Запуск ===
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

# Теперь мы берем гарантированно существующий /app/app.jar из корня сборщика!
COPY --from=builder /app/app.jar /app/app.jar

ENTRYPOINT ["java", "-jar", "/app/app.jar"]