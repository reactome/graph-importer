[<img src=https://user-images.githubusercontent.com/6883670/31999264-976dfb86-b98a-11e7-9432-0316345a72ea.png height=75 />](https://reactome.org)

# Reactome Graph Database Batch Importer

## What is the Reactome Graph Batch Importer project

The Graph Importer is a tool used for initial data conversion from the Reactome relational database to a graph database. To maximise import performance the Neo4j Graph Importer is utilised to directly create the Neo4j database file structure. This process is unsafe ignoring transactions, constraints or other safety features. Constraints will be checked once the import process has finished. 

The Gatch Importer generates the graph database dynamically depending on the Model specified in the Reactome graph library. Depending on the POJOs specified in Domain model, data will be automatically fetched from corresponding Instances of the relational database. Annotations used in the model help to indicate properties and relationships that should be taken into account in the import process.  

#### Project components used:

* [Neo4j](https://neo4j.com/download/) Community Edition - version: 3.2.2 or latest
* Reactome [Graph](https://github.com/reactome/graph-core) Core 

#### Data Import

Reactome data can be imported without the script using the Main.java entry point. Use: ```java -jar BatchImporter-jar-with-dependencies.jar```

:warning: **CAUTION:** In order for the import to succeed following steps must be ensured:
  1. All permissions to the specified target folder must be granted to the executing users of the jar file
  2. When using the new database, permissions must be given to neo4j in order to access the database.
  3. Restart neo4j 

**Properties**

When executing the jar file following properties have to be set.
```java
Usage:
  org.reactome.server.graph.Main [--help] [(-h|--host) <host>] [(-s|--port)
  <port>] [(-d|--name) <name>] [(-u|--user) <user>] [(-p|--password) <password>]
  [(-n|--neo4j) <neo4j>] [(-f|--intactFile) <intactFile>]
  [(-i|--interactions)[:<interactions>]] [(-b|--bar)[:<bar>]]

A tool for importing reactome data import to the neo4j graphDb


  [--help]
        Prints this help message.

  [(-h|--host) <host>]
        The database host (default: localhost)

  [(-s|--port) <port>]
        The reactome port (default: 3306)

  [(-d|--name) <name>]
        The reactome database name to connect to (default: reactome)

  [(-u|--user) <user>]
        The database user (default: reactome)

  [(-p|--password) <password>]
        The password to connect to the database (default: reactome)

  [(-n|--neo4j) <neo4j>]
        Path to the neo4j database (default: ./target/graph.db)

  [(-f|--intactFile) <intactFile>]
        Path to the interaction data file

  [(-i|--interactions)[:<interactions>]]
        Include interaction data. If the intactFile is not provided, the
        interaction data will be downloaded
```

Example:
```java
java -jar GraphImporter-jar-with-dependencies \ 
     -h localhost \ 
     -s 3306 \
     -t reactome \ 
     -u reactome_user \
     -p not2share \ 
     -d ./target/graph.db
```

#### Extras
* [1] [Reactome Graph Database](http://www.reactome.org/download/current/reactome.graphdb.tgz)
* [2] [Documentation](http://www.reactome.org/pages/documentation/developer-guide/graph-database/)
* [3] [MySQL dump database](http://www.reactome.org/download/current/databases/gk_current.sql.gz)
