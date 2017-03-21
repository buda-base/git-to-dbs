# fuseki-couchdb

CouchDB - Fuseki interface (specific to BDRC data)

This repository has a [submodule](http://paynedigital.com/articles/2011/10/introduction-to-git-submodules).

## Compiling and running

Simple run:

```
mvn compile exec:java -Dexec.args="-fusekiHost <host> -fusekiPort <port> -couchdbHost <host> -couchdbPort <port>"
```

Compiling and generating jar file:

```
mvn clean package
```

Running the jar file:

```
java -jar target/fusekicouchdb-0.1.jar -fusekiHost <host> -fusekiPort <port> -couchdbHost <host> -couchdbPort <port>
```