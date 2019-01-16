#!/bin/bash

export GITS="../bdrc-gits"
time java -jar target/gittodbs-0.8.0.jar -fuseki -transferOnto -timeout 60 -progress -gitDir ${GITS} -n 0