# fuseki-couchdb

CouchDB - Fuseki interface (specific to BDRC data)

After cloning: 

```
git submodule update --init
```

## Compiling and running

Simple run:

```
mvn compile exec:java -Dexec.args="-help"
```

Compiling and generating jar file:

```
mvn clean package
```

Running the jar file:

```
java -jar target/fusekicouchdb-0.1.jar -help
```