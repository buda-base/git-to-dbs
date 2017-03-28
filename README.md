# fuseki-couchdb

This repository contains code to transfer BDRC data from CouchDB to Fuseki. It can transfer the whole database or just listen to changes in CouchDB.

## Downloading

You can either clone this repository (and then the submodule: `git submodule update --init`) or fetch the latest .jar file on the [release page](https://github.com/BuddhistDigitalResourceCenter/fuseki-couchdb/releases). You can automatically get the jar file of the latest release with

```
curl -s https://api.github.com/repos/BuddhistDigitalResourceCenter/fuseki-couchdb/releases/latest | jq -r ".assets[] | select(.name | test(\"jar$\")) | .browser_download_url" | xargs curl -sL -o fusekicouchdb-latest.jar -O
```

## Compiling and running

##### from the jar file 

```
java -jar target/fusekicouchdb-0.2.jar -help
```

##### from the java code

```
mvn compile exec:java -Dexec.args="-help"
```

To compile a jar file:

```
mvn clean package
```
