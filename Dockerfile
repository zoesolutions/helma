FROM openjdk:8
MAINTAINER Daniel Ruthardt <dr@zoesolutions.eu>

# copy Helma to the docker image
COPY . /opt/helma

# update the packages database
RUN apt-get update
# install ant
RUN apt-get install -y ant

# go to Helma's directory
WORKDIR /opt/helma
# build Helma
RUN ant jar

# expose the web port
EXPOSE 8080

# export the apps, log, db and external libraries directory as volume
VOLUME ["/opt/helma/apps", "/opt/helma/log", "/opt/helma/db", "/opt/helma/lib/ext"]

# run Helma
CMD ["/opt/helma/start.sh"]
