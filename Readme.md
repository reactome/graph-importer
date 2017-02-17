<img src=https://cloud.githubusercontent.com/assets/6883670/22938783/bbef4474-f2d4-11e6-92a5-07c1a6964491.png width=220 height=100 />

# Reactome Graph Database Batch Importer

## What is the Reactome Graph Batch Importer project

The Batch Importer is a tool used for initial data conversion from the Reactome relational database to a graph database. To maximise import performance the Neo4j Batch Importer is utilised to directly create the Neo4j database file structure. This process is unsafe ignoring transactions, constraints or other safety features. Constraints will be checked once the import process has finished. 

The Batch Importer generates the graph database dynamically depending on the Model specified in the Reactome graph library. Depending on the POJOs specified in Domain model, data will be automatically fetched from corresponding Instances of the relational database. Annotations used in the model help to indicate properties and relationships that should be taken into account in the import process.  

#### Project components used:

* [Neo4j](https://neo4j.com/download/) Community Edition - version: 3.0.1 or latest
* Reactome [Graph](https://github.com/reactome/graph-core) Core 

#### Reactome data import

Reactome data will be automatically be imported when running the [script](https://raw.githubusercontent.com/reactome/graph-importer/master/setup-graph.sh) ```setup-graph.sh```. User executing this script will be asked for password if permissions require it.

Another option could be cloning the git repository ```git clone https://github.com/reactome/graph-importer.git``` 
 
* Script Usage
```console
./setup-graph 
    -h  Print usage.
    -i  Add -i to Install Neo4j
    -j  Add -j to Importa Data into Neo4j
    -h  Reactome MySQL database host. DEFAULT: localhost
    -s  Reactome MySQL database port. DEFAULT: 3306
    -t  Reactome MySQL database name. DEFAULT: reactome
    -u  Reactome MySQL database user. DEFAULT: reactome
    -v  Reactome MySQL database password. DEFAULT: reactome
    -d  Target directory where graph will be created DEFAULT: ./target/graph.db
    -n  Neo4j password (only set when neo4j is installed)
```

:warning: Do not execute as sudo, permission will be asked when required

* Installing Neo4j (Linux only)
```console
./setup-graph -i
```

* Installing Neo4j in other platforms
    * [MAC OS X](http://neo4j.com/docs/operations-manual/current/installation/osx/)
    * [Windows](http://neo4j.com/docs/operations-manual/current/installation/windows/)
    
```console
By opening http://localhost:7474 and reaching Neo4j browser you're ready to import data.
```

* Importing Data

> :memo: Refer to [Extras](https://github.com/gsviteri/DemoLayout/new/master?readme=1#extras) in order to download the MySql database before starting.

```console
./setup-graph -j 
    -h  Reactome MySQL database host. DEFAULT: localhost
    -s  Reactome MySQL database port. DEFAULT: 3306
    -t  Reactome MySQL database name. DEFAULT: reactome
    -u  Reactome MySQL database user. DEFAULT: reactome
    -v  Reactome MySQL database password. DEFAULT: reactome
    -d  Target directory where graph will be created DEFAULT: ./target/graph.db
    -n  Neo4j password (only set when neo4j is installed)
```

Example:
```
./setup-graph -j -h localhost -s 3306 -t reactome -u reactome_user -p not2share -d ./target/graph.db
```

#### Data Import without the script

Reactome data can be imported without the script using the Main.java entry point. Use: ```java -jar BatchImporter-jar-with-dependencies.jar```

:warning: **CAUTION:** In order for the import to succeed following steps must be ensured:
  1. All permissions to the specified target folder must be granted to the executing users of the jar file
  2. When using the new database, permissions must be given to neo4j in order to access the database.
  3. Restart neo4j 

**Properties**

When executing the jar file following properties have to be set.
```java
    -h  Reactome MySQL database host. DEFAULT: localhost
    -s  Reactome MySQL database port. DEFAULT: 3306
    -t  Reactome MySQL database name. DEFAULT: reactome
    -u  Reactome MySQL database user. DEFAULT: reactome
    -v  Reactome MySQL database password. DEFAULT: reactome
    -d  Target directory where graph will be created DEFAULT: ./target/graph.db
    -n  Neo4j password (only set when neo4j is installed)
```

Example:
```java
java -jar BatchImporter-jar-with-dependencies.jar \ 
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