![Logo](https://cdn.evbuc.com/images/3621635/40070539972/1/logo.png)

# Reactome Graph Database Batch Importer

## What is the Reactome Graph Batch Importer project

The Batch Importer is a tool used for initial data conversion from the Reactome MqSQL database to a graph database. To maximise import performance the Neo4j Batch Importer is utilised to directly create the Neo4j database file structure. This process is unsafe ignoring transactions, constraints or other safety features. Constraints will be checked once the import process has finished. 
The BatchImporter generates the graph database dynamically depending on the Model specified in the Reactome graph library. Depending on the POJOs specified in Domain model, data will be automatically fetched from corresponding Instances of the MySQL database. Annotations used in the model help to indicate properties and relationships that should be taken into account in the import process.  

#### Project components used:

* Neo4j - version: 3.0.1
* Reactome graph library 

#### Project usage: 

The Batch importer can be executed using the setup-graph.sh script or manually running the executable jar file. 

#### Reactome data import

Reactome data will be automatically be imported when running the ```setup-graph.sh``` script. User executing this script will be asked for password if permissions require it.

#### Data Import without the script

Reactome data can be imported without the script using the DataImportLauncher entry point. Use: ```java-jar DataImport.jar```
**CAUTION!**
In order for the import to succeed following steps must be ensured:
 1) All permissions to the specified target folder must be granted to the executing users of the jar file
 2) When using the new database, permissions must be given to neo4j in order to access the database.
 3) Restart neo4j 

**Properties**

When executing the jar file following properties have to be set.

    -h  Reactome MySQL database host. DEFAULT: localhost
    -s  Reactome MySQL database port. DEFAULT: 3306
    -s  Reactome MySQL database name. DEFAULT: reactome
    -u  Reactome MySQL database user. DEFAULT: reactome
    -v  Reactome MySQL database password. DEFAULT: reactome
    -d  Target directory where graph will be created DEFAULT: ./target/graph.db