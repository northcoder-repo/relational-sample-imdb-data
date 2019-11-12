#! /bin/bash

$JAVA_HOME/bin/java \
  -cp bin/h2-1.4.200.jar org.h2.tools.RunScript \
  -url jdbc:h2:tcp://localhost:9092/imdb \
  -user sa \
  -password your_pw_here \
  -showResults \
  -script h2_imdb_create_and_load.sql
 