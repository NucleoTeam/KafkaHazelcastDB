FROM openjdk:11-stretch

COPY ./gradlew /srv/gradlew

COPY ./gradle /srv/gradle
COPY ./build.gradle /srv/build.gradle
COPY ./settings.gradle /srv/settings.gradle

RUN cd /srv/;./gradlew wrapper

COPY ./src /srv/src

RUN cd /srv/;./gradlew build
RUN cd /srv/build/distributions/;unzip ./database*.zip
RUN mkdir /server/;cd /srv/build/distributions/database*;cp -r ./* /server

WORKDIR /server/
CMD bin/database
