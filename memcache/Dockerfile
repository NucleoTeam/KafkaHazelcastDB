FROM docker.io/openjdk:20-slim-bullseye as builder

ADD ./ /

RUN cd /;./gradlew :memcache:shadowJar
RUN cd /;ls /memcache/build/libs/

FROM docker.io/openjdk:22-slim-bullseye

COPY --from=builder /memcache/build/libs/Shadow-*.jar /

ENV PORT=11211
ENV KAFKA_HOSTS=kafka:29092
ENV KAFKA_TOPIC=memcache

CMD java -jar  /Shadow-*.jar
