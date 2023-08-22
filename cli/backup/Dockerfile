FROM docker.io/openjdk:20-slim-bullseye as builder

ADD ./ /

RUN cd /;./gradlew :cli:backup:shadowJar

FROM docker.io/openjdk:22-slim-bullseye

COPY --from=builder /cli/backup/build/libs/Shadow-*.jar /

# CLASS PATH
ENV TABLE_CLASS="com.nucleocore.library.test.User"

# FROM TABLE
ENV SOURCE_KAFKA_HOSTS=kafka:29092
ENV SOURCE_TEMP_TABLE=users

# TO TABLE
ENV TARGET_TABLE=users

CMD java -jar  /Shadow-*.jar
