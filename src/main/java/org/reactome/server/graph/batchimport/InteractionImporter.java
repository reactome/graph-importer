package org.reactome.server.graph.batchimport;

import org.apache.commons.lang.StringUtils;
import org.gk.model.GKInstance;
import org.gk.model.ReactomeJavaConstants;
import org.gk.persistence.MySQLAdaptor;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.reactome.server.graph.domain.model.ReferenceGeneProduct;
import org.reactome.server.graph.domain.model.ReferenceMolecule;
import org.reactome.server.graph.domain.model.UndirectedInteraction;
import org.reactome.server.interactors.IntactParser;
import org.reactome.server.interactors.database.InteractorsDatabase;
import org.reactome.server.interactors.model.Interaction;
import org.reactome.server.interactors.model.InteractorResource;
import org.reactome.server.interactors.service.InteractionService;
import org.reactome.server.interactors.service.InteractorResourceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import static org.reactome.server.graph.batchimport.ReactomeBatchImporter.ORDER;
import static org.reactome.server.graph.batchimport.ReactomeBatchImporter.STOICHIOMETRY;

class InteractionImporter {

    private static final Logger importLogger = LoggerFactory.getLogger("import");

    private MySQLAdaptor dba;

    private InteractionService interactionService;
    private InteractorResourceService interactorResourceService;
    private static final Map<String, Long> referenceEntityMap = new HashMap<>(); // (UniProt:12345) -> (dbId)

    InteractionImporter(MySQLAdaptor dba) {
        this.dba = dba;
        try {
            System.out.print("Retrieving interaction data...");
            importLogger.info("Retrieving interaction data");
            InteractorsDatabase interactorsDatabase = IntactParser.getInteractors();
            interactionService = new InteractionService(interactorsDatabase);
            interactorResourceService = new InteractorResourceService(interactorsDatabase);
            importLogger.info("Interaction data retrieved");
            System.out.println("\rInteraction data retrieved");
        } catch (SQLException | IOException e) {
            importLogger.error("An error occurred while retrieving the interaction data", e);
        }
    }

    void addReferenceEntity(String identifier, GKInstance instance){
        referenceEntityMap.put(identifier, instance.getDBID());
    }

    void addInteractionData(Map<Long, Long> dbIds, BatchInserter batchInserter, Long maxDbId) {

        System.out.print("\n\nAdding interaction data");

        Set<String> existingInteractions = new HashSet<>();

        int added = 0;
        try {
            int i = 0; long tot = dba.getClassInstanceCount(ReactomeJavaConstants.ReferenceEntity);
            for (Object aux : dba.fetchInstancesByClass(ReactomeJavaConstants.ReferenceEntity)) {
                GKInstance referenceEntity = (GKInstance) aux;
                System.out.print(String.format("\rAdding interaction data: (%,d/%,d) >> %s",++i, tot, referenceEntity.getDisplayName()));
                if(!dbIds.containsKey(referenceEntity.getDBID())) continue;
                String identifier = (String) ReactomeBatchImporter.getObjectFromGkInstance(referenceEntity, "identifier");
                if (identifier != null) {
                    for (Interaction intactInteraction : interactionService.getInteractions(identifier, "static")) {
                        InteractorResource resourceA = interactorResourceService.getAllMappedById().get(intactInteraction.getInteractorA().getInteractorResourceId());

                        List<String> interactors = new LinkedList<>();
                        interactors.add(intactInteraction.getInteractorA().getAcc() + "#" + intactInteraction.getInteractorA().getInteractorResourceId());
                        interactors.add(intactInteraction.getInteractorB().getAcc() + "#" + intactInteraction.getInteractorB().getInteractorResourceId());
                        Collections.sort(interactors);
                        String ab = StringUtils.join(interactors, "<>");
                        if(existingInteractions.contains(ab)) continue;
                        existingInteractions.add(ab);

                        Map<String, Object> interaction = new HashMap<>();
                        interaction.put("dbId", ++maxDbId);
                        interaction.put("displayName", ab);
                        interaction.put("databaseName", resourceA.getName());
                        interaction.put("score", intactInteraction.getIntactScore());
                        Long fromId = batchInserter.createNode(interaction, ReactomeBatchImporter.getLabels(UndirectedInteraction.class));
                        dbIds.put(maxDbId, fromId);

                        //Add interactorA
                        RelationshipType relationshipType = RelationshipType.withName("interactor");
                        Map<String, Object> properties = new HashMap<>();
                        properties.put(STOICHIOMETRY, 1);
                        properties.put(ORDER, 1);
                        ReactomeBatchImporter.saveRelationship(dbIds.get(referenceEntity.getDBID()), fromId, relationshipType, properties);

                        Long target = referenceEntityMap.get(intactInteraction.getInteractorB().getAcc());

                        Long toId;
                        if (target != null) {
                            toId = dbIds.get(target);
                        } else {
                            InteractorResource resourceB = interactorResourceService.getAllMappedById().get(intactInteraction.getInteractorB().getInteractorResourceId());

                            identifier = intactInteraction.getInteractorB().getAcc();
                            Map<String, Object> toReferenceEntity = new HashMap<>();
                            toReferenceEntity.put("dbId", ++maxDbId);
                            toReferenceEntity.put("displayName", resourceB.getName() + ":" + identifier);
                            toReferenceEntity.put("databaseName", resourceB.getName());
                            toReferenceEntity.put("identifier", identifier);
                            //referenceEnity.put("url", null); //TODO
                            //crossReference //TODO
                            //referenceDatabase //TODO
                            Label[] labels = ReactomeBatchImporter.getLabels(resourceB.getName().toLowerCase().equals("uniprot") ? ReferenceGeneProduct.class : ReferenceMolecule.class);
                            toId = batchInserter.createNode(toReferenceEntity, labels);
                            dbIds.put(maxDbId, toId);
                            referenceEntityMap.put(identifier, maxDbId);
                        }

                        //Add interactorB
                        properties.put(ORDER, 2);
                        ReactomeBatchImporter.saveRelationship(toId, fromId, relationshipType, properties);

                        added++;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(String.format("\r%,d interactions have been added to the graph.", added));
    }
}
