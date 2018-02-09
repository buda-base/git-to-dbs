#!/usr/bin/env bash

# run like:   migrate-all.sh ~/git/git-to-dbs /usr/local/BUDA_TECH/bdrc-gits /usr/local/BUDA_TECH

export REPO=$1
export GITS=$2
export LOGS=$3

echo "Transferring Persons"
time java -jar $REPO/target/gittodbs-0.7.jar -fuseki -transferOnto -timeout 120 -progress -gitDir $GITS/ -type person > $LOGS/GIT2DBS-person.txt 2>&1
echo "Transferring Items"
time java -jar $REPO/target/gittodbs-0.7.jar -fuseki -timeout 120 -progress -gitDir $GITS/ -type item > $LOGS/GIT2DBS-item.txt 2>&1
echo "Transferring Works"
time java -jar $REPO/target/gittodbs-0.7.jar -fuseki -timeout 120 -progress -gitDir $GITS/ -type work > $LOGS/GIT2DBS-work.txt 2>&1
echo "Transferring Etexts"
time java -jar $REPO/target/gittodbs-0.7.jar -fuseki -timeout 120 -progress -gitDir $GITS/ -type etext > $LOGS/GIT2DBS-etext.txt 2>&1
echo "Transferring Corporations"
time java -jar $REPO/target/gittodbs-0.7.jar -fuseki -timeout 120 -progress -gitDir $GITS/ -type corporation > $LOGS/GIT2DBS-corporation.txt 2>&1
echo "Transferring Places"
time java -jar $REPO/target/gittodbs-0.7.jar -fuseki -timeout 120 -progress -gitDir $GITS/ -type place > $LOGS/GIT2DBS-place.txt 2>&1
echo "Transferring Topics"
time java -jar $REPO/target/gittodbs-0.7.jar -fuseki -timeout 120 -progress -gitDir $GITS/ -type topic > $LOGS/GIT2DBS-topic.txt 2>&1
echo "Transferring Lineages"
time java -jar $REPO/target/gittodbs-0.7.jar -fuseki -timeout 120 -progress -gitDir $GITS/ -type lineage > $LOGS/GIT2DBS-lineage.txt 2>&1
echo "Transferring Products"
time java -jar $REPO/target/gittodbs-0.7.jar -fuseki -timeout 120 -progress -gitDir $GITS/ -type product > $LOGS/GIT2DBS-product.txt 2>&1
echo "Transferring Offices"
time java -jar $REPO/target/gittodbs-0.7.jar -fuseki -timeout 120 -progress -gitDir $GITS/ -type office > $LOGS/GIT2DBS-office.txt 2>&1
echo "Transferring Etext Contents"
time java -jar $REPO/target/gittodbs-0.7.jar -fuseki -timeout 120 -progress -gitDir $GITS/ -type etextcontent > $LOGS/GIT2DBS-etextcontent.txt 2>&1
