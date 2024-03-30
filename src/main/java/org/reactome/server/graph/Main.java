package org.reactome.server.graph;

import com.martiansoftware.jsap.*;
import org.reactome.server.graph.batchimport.ReactomeBatchImporter;

import java.io.IOException;

/**
 * @author Florian Korninger (florian.korninger@ebi.ac.uk)
 */
public class Main {

    public static void main(String[] args) throws JSAPException, IOException {


        SimpleJSAP jsap = new SimpleJSAP(Main.class.getName(), "A tool for importing reactome data import to the neo4j graphDb",
                new Parameter[]{
                        new FlaggedOption(  "host",         JSAP.STRING_PARSER,   "localhost",          JSAP.NOT_REQUIRED, 'h', "host",         "The database host"),
                        new FlaggedOption(  "port",         JSAP.INTEGER_PARSER,  "3306",               JSAP.NOT_REQUIRED, 's', "port",         "The reactome port"),
                        new FlaggedOption(  "name",         JSAP.STRING_PARSER,   "reactome",           JSAP.NOT_REQUIRED, 'd', "name",         "The reactome database name to connect to"),
                        new FlaggedOption(  "user",         JSAP.STRING_PARSER,   "reactome",           JSAP.NOT_REQUIRED, 'u', "user",         "The database user"),
                        new FlaggedOption(  "password",     JSAP.STRING_PARSER,   "reactome",           JSAP.NOT_REQUIRED, 'p', "password",     "The password to connect to the database"),
                        new FlaggedOption(  "neo4j",        JSAP.STRING_PARSER,   "./target/graph.db",  JSAP.NOT_REQUIRED, 'n', "neo4j",        "Path to the neo4j database"),
                        new FlaggedOption(  "neo4jVersion", JSAP.STRING_PARSER,   ">= 3.5.x",           JSAP.NOT_REQUIRED, 'r', "neo4jVersion", "Neo4j version"),
                        new QualifiedSwitch("bar",          JSAP.BOOLEAN_PARSER,  JSAP.NO_DEFAULT,          JSAP.NOT_REQUIRED, 'b', "bar",          "Forces final status")
                }
        );

        JSAPResult config = jsap.parse(args);
        if (jsap.messagePrinted()) System.exit(1);

        /*
         * @Autowired annotation does not work in a static context. context.getBean has to be used instead.
         * final AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(MyConfiguration.class);
         * ReactomeBatchImporter batchImporter = ctx.getBean(ReactomeBatchImporter.class);
         */
        ReactomeBatchImporter batchImporter = new ReactomeBatchImporter(
                config.getString("host"),
                config.getInt("port"),
                config.getString("name"),
                config.getString("user"),
                config.getString("password"),
                config.getString("neo4j"),
                config.getString("neo4jVersion")
        );
        batchImporter.importAll(!config.getBoolean("bar"));
    }
}