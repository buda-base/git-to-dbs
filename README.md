# git-to-dbs

This repository contains code to transfer BDRC data from local git repos to Fuseki and/or couchdb. It can transfer the whole database or just listen to changes in the git repos.

## Downloading

You can either clone this repository (and then the submodule: `git submodule update --init`) or fetch the latest .jar file on the [release page](https://github.com/BuddhistDigitalResourceCenter/fuseki-couchdb/releases). You can automatically get the jar file of the latest release with

```
curl -s https://api.github.com/repos/BuddhistDigitalResourceCenter/fuseki-couchdb/releases/latest | jq -r ".assets[] | select(.name | test(\"jar$\")) | .browser_download_url" | xargs curl -sL -o fusekicouchdb-latest.jar -O
```

## Compiling and running

##### from the jar file 

```
java -jar target/gittodbs-0.7.jar -help
```

A typical usage is:

```
time java -jar target/gittodbs-0.7.jar -transferOnto -transferAllDB -doNotListen -timeout 60 -progress
```

##### from the java code

```
mvn compile exec:java -Dexec.args="-help"
```

To compile a jar file:

```
mvn clean package
```
or
```
mvn clean package -Dmaven.test.skip=true
```
When there is a change in the owl-schema repo the following is needed:

```
git submodule update --init
```

