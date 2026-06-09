# Stage 1: Build the application using the full JDK
FROM eclipse-temurin:26-jdk-alpine AS builder
WORKDIR /app

# Copy the Maven wrapper and project files
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
COPY src ./src

# Make the wrapper executable and build the .jar
RUN chmod +x mvnw
RUN ./mvnw clean package -DskipTests

# Stage 2: Run the application using the lean JRE
FROM eclipse-temurin:26-jre-alpine
WORKDIR /app

# Copy ONLY the compiled .jar from the builder stage
COPY --from=builder /app/target/*.jar app.jar

ENTRYPOINT ["java", "-jar", "app.jar"]%      