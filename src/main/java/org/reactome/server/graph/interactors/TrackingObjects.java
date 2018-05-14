package org.reactome.server.graph.interactors;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.reactome.server.graph.batchimport.ReactomeBatchImporter;
import org.reactome.server.graph.domain.model.InstanceEdit;
import org.reactome.server.graph.domain.model.Person;
import org.reactome.server.graph.domain.model.ReferenceDatabase;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.reactome.server.graph.batchimport.ReactomeBatchImporter.*;

class TrackingObjects {

    private static RelationshipType author = RelationshipType.withName("author");
    private static RelationshipType created = RelationshipType.withName("created");
//    private static RelationshipType modified = RelationshipType.withName("modified");

    private static final DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static final Map<String, Object> properties;
    static {
        properties = new HashMap<>();
        properties.put(STOICHIOMETRY, 1);
        properties.put(ORDER, 1);
    }

    static Long createIntActReferenceDatabase(Map<Long, Long> dbIds, Long graphImporterUserNode, BatchInserter batchInserter) {
        Class schemaClass = ReferenceDatabase.class;
        Map<String, Object> intact = new HashMap<>();
        intact.put("dbId", ++maxDbId);
        intact.put("displayName", "IntAct");
        intact.put("name", Collections.singletonList("IntAct").toArray(new String[1]));
        intact.put("schemaClass", schemaClass.getSimpleName());
        intact.put("url", "https://www.ebi.ac.uk/intact");
        intact.put("accessUrl", "https://www.ebi.ac.uk/intact/query/###ID###");
        Long id = batchInserter.createNode(intact, ReactomeBatchImporter.getLabels(schemaClass));
        addCreatedModified(id, graphImporterUserNode, batchInserter);
        dbIds.put(maxDbId, id);
        return maxDbId;
    }

    static Long createGraphImporterUserNode(BatchInserter batchInserter) {
        Class schemaClass = Person.class;
        Map<String, Object> grapUserNode = new HashMap<>();
        grapUserNode.put("dbId", ++maxDbId);
        grapUserNode.put("displayName", "Interactions Importer");
        grapUserNode.put("firstname", "Interactions Importer");
        grapUserNode.put("surname", "Script");
        grapUserNode.put("initial", "AF");
        grapUserNode.put("schemaClass", schemaClass.getSimpleName());
        return batchInserter.createNode(grapUserNode, ReactomeBatchImporter.getLabels(schemaClass));
    }

    static void addCreatedModified(Long node, Long graphImporterUserNode, BatchInserter batchInserter) {
        Long c = createInstanceEditNode(graphImporterUserNode, batchInserter);
        ReactomeBatchImporter.saveRelationship(c, node, created, properties);

//        Long m = createInstanceEditNode(graphImporterUserNode, batchInserter);
//        ReactomeBatchImporter.saveRelationship(m, node, modified, properties);
    }

    private static Long createInstanceEditNode(Long graphImporterUserNode, BatchInserter batchInserter) {
        Class schemaClass = InstanceEdit.class;
        String dateTime = formatter.format(new Date());
        Map<String, Object> instanceEdit = new HashMap<>();
        instanceEdit.put("dbId", ++maxDbId);
        instanceEdit.put("displayName", "Interactions Importer, " + dateTime);
        instanceEdit.put("dateTime", dateTime);
        instanceEdit.put("schemaClass", schemaClass.getSimpleName());
        Long id = batchInserter.createNode(instanceEdit, ReactomeBatchImporter.getLabels(schemaClass));
        ReactomeBatchImporter.saveRelationship(graphImporterUserNode, id, author, properties);
        return id;
    }
}
