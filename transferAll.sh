#!/bin/bash

java -jar target/fusekicouchdb-0.7.jar -transferAllDB -progress -doNotListen -couchdbName bdrc_corporation
java -jar target/fusekicouchdb-0.7.jar -transferAllDB -progress -doNotListen -couchdbName bdrc_lineage
java -jar target/fusekicouchdb-0.7.jar -transferAllDB -progress -doNotListen -couchdbName bdrc_office
java -jar target/fusekicouchdb-0.7.jar -transferAllDB -progress -doNotListen -couchdbName bdrc_outline
java -jar target/fusekicouchdb-0.7.jar -transferAllDB -progress -doNotListen -couchdbName bdrc_person
java -jar target/fusekicouchdb-0.7.jar -transferAllDB -progress -doNotListen -couchdbName bdrc_place
java -jar target/fusekicouchdb-0.7.jar -transferAllDB -progress -doNotListen -couchdbName bdrc_topic
java -jar target/fusekicouchdb-0.7.jar -transferAllDB -progress -doNotListen -couchdbName bdrc_volumes
java -jar target/fusekicouchdb-0.7.jar -transferAllDB -progress -doNotListen -couchdbName bdrc_work
