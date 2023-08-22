FROM openjdk:22-slim-bullseye
RUN apt update && apt install -y wget
RUN cd /;wget https://github.com/NucleoTeam/NucleoDB/releases/download/1.05.19/memcache-1.05.19.jar
WORKDIR /
CMD java -jar memcache-1.05.19.jar
