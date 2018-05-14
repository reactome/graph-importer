package org.reactome.server.graph.interactors;

import org.apache.commons.io.FileUtils;
import org.gk.model.GKInstance;
import org.gk.model.ReactomeJavaConstants;
import org.gk.persistence.MySQLAdaptor;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.reactome.server.graph.batchimport.ReactomeBatchImporter;
import org.reactome.server.graph.domain.model.ReferenceGeneProduct;
import org.reactome.server.graph.domain.model.ReferenceIsoform;
import org.reactome.server.graph.domain.model.ReferenceMolecule;
import org.reactome.server.graph.domain.model.UndirectedInteraction;
import org.reactome.server.graph.utils.TaxonomyHelper;
import org.reactome.server.interactors.IntactParser;
import org.reactome.server.interactors.database.InteractorsDatabase;
import org.reactome.server.interactors.exception.InvalidInteractionResourceException;
import org.reactome.server.interactors.model.Interaction;
import org.reactome.server.interactors.model.Interactor;
import org.reactome.server.interactors.model.InteractorResource;
import org.reactome.server.interactors.service.InteractionService;
import org.reactome.server.interactors.service.InteractorResourceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import static org.reactome.server.graph.batchimport.ReactomeBatchImporter.*;
import static org.reactome.server.graph.utils.FormatUtils.getTimeFormatted;

/**
 * Imports interaction data from the IntAct database.
 *
 * Uses the interactors-core project (https://github.com/reactome-pwp/interactors-core)
 *
 * @author Antonio Fabregat (fabregat@ebi.ac.uk)
 */
public class InteractionImporter {

    private static final Logger importLogger = LoggerFactory.getLogger("import");
    private static final int PROGRESS_BAR_WITH = 70;

    private MySQLAdaptor dba;
    private Map<Long, Long> dbIds;

    private TaxonomyHelper taxonomyHelper;

    private static final Long REACTOME_UNIPROT_REFERENCE_DATABASE = 2L;
    private static final Long REACTOME_CHEBI_REFERENCE_DATABASE = 114984L;

    private static Boolean useUserInteractionData;
    private static String userInteractionDataFile;
    private static final String INTERACTION_DATA_TMP_FILE = "./interaction-data.tmp.db";
    private static final Integer QUERIES_OFFSET = 1000;
    private InteractorsDatabase interactorsDatabase;
    private InteractionService interactionService;
    private InteractorResourceService interactorResourceService;

    private Long intActReferenceDatabaseDbId;
    private static final Map<String, Set<Long>> referenceEntityMap = new HashMap<>(); // (UniProt:12345) -> [dbId]

    public InteractionImporter(MySQLAdaptor dba, Map<Long, Long> dbIds, Map<Integer, Long> taxIdDbId, String fileName) {
        this.dba = dba;
        this.dbIds = dbIds;
        this.taxonomyHelper = new TaxonomyHelper(taxIdDbId);
        useUserInteractionData = fileName != null && !fileName.isEmpty();
        userInteractionDataFile = fileName;
    }

    public void addInteractionData(BatchInserter batchInserter) {
        Long start = System.currentTimeMillis();
        initialise();

        RelationshipType interactor = RelationshipType.withName("interactor");
        RelationshipType referenceDatabase = RelationshipType.withName("referenceDatabase");
        RelationshipType species = RelationshipType.withName("species");

        Map<String, Object> stdRelationshipProp = new HashMap<>();
        stdRelationshipProp.put(STOICHIOMETRY, 1);
        stdRelationshipProp.put(ORDER, 1);

        Long graphImporterUserNode = TrackingObjects.createGraphImporterUserNode(batchInserter);
        intActReferenceDatabaseDbId = TrackingObjects.createIntActReferenceDatabase(dbIds, graphImporterUserNode, batchInserter);
        Long intActReferenceDatabaseNode = dbIds.get(intActReferenceDatabaseDbId);

        Set<Long> addedInteractions = new HashSet<>();
        int addedReferenceEntities = 0;
        Collection<GKInstance> referenceEntities = getTargetReferenceEntities();
        long i = 0; int total = referenceEntities.size();
        for (GKInstance referenceEntity : referenceEntities) {
            updateProgressBar(++i, total);
            if(i % QUERIES_OFFSET == 0) cleanInteractorsCache();
            final Long a = dbIds.get(referenceEntity.getDBID());
            if (a == null) continue;

            String sourceIdentifier = (String) ReactomeBatchImporter.getObjectFromGkInstance(referenceEntity, "variantIdentifier");
            if (sourceIdentifier == null) sourceIdentifier = (String) ReactomeBatchImporter.getObjectFromGkInstance(referenceEntity, "identifier");
            if (sourceIdentifier != null) {
                String resource = getReferenceDatabaseName(referenceEntity);
                for (Interaction intactInteraction : getIntActInteraction(resource, sourceIdentifier)) {

                    final String targetIdentifier = intactInteraction.getInteractorB().getAcc().trim().split(" ")[0];

                    final Set<Long> targetEntities = referenceEntityMap.get(targetIdentifier);

                    final List<Long> targetNodes = new ArrayList<>();
                    if (targetEntities != null && !targetEntities.isEmpty())  {
                        targetEntities.forEach(t -> {
                            if (dbIds.containsKey(t)) targetNodes.add(dbIds.get(t));
                        });
                    } else {
                        Interactor ib = intactInteraction.getInteractorB();
                        Map<String, Object> toReferenceEntity = createReferenceEntityMap(ib);
                        Long dbId = (Long) toReferenceEntity.get("dbId");
                        Long refDbNode = (Long) toReferenceEntity.remove("referenceDatabaseNode");
                        Label[] labels = (Label[]) toReferenceEntity.remove("labels");
                        Long b = batchInserter.createNode(toReferenceEntity, labels);
                        TrackingObjects.addCreatedModified(b, graphImporterUserNode, batchInserter);
                        dbIds.put(dbId, b);
                        targetNodes.add(b);
                        referenceEntityMap.computeIfAbsent(targetIdentifier, k -> new HashSet<>()).add(dbId);
                        ReactomeBatchImporter.saveRelationship(refDbNode, b, referenceDatabase, stdRelationshipProp);
                        //Adding species relationship when exists
                        Long speciesDbId = taxonomyHelper.getTaxonomyLineage(ib.getTaxid());
                        if (speciesDbId != null) {
                            Long speciesNode = dbIds.get(speciesDbId);
                            if (speciesNode != null) {
                                ReactomeBatchImporter.saveRelationship(speciesNode, b, species, stdRelationshipProp);
                                importLogger.info("species " + speciesDbId + " added to " + dbId);
                            }
                        }
                        addedReferenceEntities++;
                    }

                    String sourceName = resource + ":" + sourceIdentifier;
                    String interactionName =  sourceName + " <-> " + targetIdentifier + " (IntAct)";
                    for (Long b : targetNodes) {
                        //Check whether the interaction has been added before
                        if (addedInteractions.contains(intactInteraction.getId())) continue;
                        addedInteractions.add(intactInteraction.getId());

                        //Add interaction instance (UndirectedInteraction)
                        Long dbId = ++maxDbId;
                        Map<String, Object> interaction = createInteractionMap(dbId, interactionName, intactInteraction);
                        Long interactionNode = batchInserter.createNode(interaction, ReactomeBatchImporter.getLabels(UndirectedInteraction.class));
                        ReactomeBatchImporter.saveRelationship(intActReferenceDatabaseNode, interactionNode, referenceDatabase, stdRelationshipProp);
                        TrackingObjects.addCreatedModified(interactionNode, graphImporterUserNode, batchInserter);
                        dbIds.put(dbId, interactionNode);

                        //Add interaction source (A)
                        Map<String, Object> properties = new HashMap<>();
                        properties.put(STOICHIOMETRY, 1);
                        properties.put(ORDER, 1);
                        ReactomeBatchImporter.saveRelationship(a, interactionNode, interactor, properties);

                        //Add interaction target (B)
                        properties.put(ORDER, 2);
                        ReactomeBatchImporter.saveRelationship(b, interactionNode, interactor, properties);
                    }
                }
            }
        }

        finalise();
        Long time = System.currentTimeMillis() - start;
        System.out.println(String.format(
                "\n\t%,d interactions and %,d ReferenceEntity objects have been added to the graph (%s). ",
                addedInteractions.size(),
                addedReferenceEntities,
                getTimeFormatted(time)
        ));
    }

    private Map<String, Object> createInteractionMap(Long dbId, String name, Interaction interaction){
        String interactionURL = "https://www.ebi.ac.uk/intact/pages/interactions/interactions.xhtml?query=";
        List<String> accession = new ArrayList<>();
        interaction.getInteractionDetailsList().forEach(details -> accession.add(details.getInteractionAc()));

        Map<String, Object> rtn = new HashMap<>();
        rtn.put("dbId", dbId);
        rtn.put("displayName", name);
        rtn.put("databaseName", "IntAct");
        rtn.put("score", interaction.getIntactScore());
        rtn.put("accession", accession.toArray(new String[accession.size()]));
        rtn.put("url", interactionURL + String.join("%20OR%20", accession));
        rtn.put("schemaClass", UndirectedInteraction.class.getSimpleName());
        return rtn;
    }

    private Map<String, Object> createReferenceEntityMap(Interactor interactor){
        InteractorResource resource = getInteractorResource(interactor);
        String identifier = interactor.getAcc().split(" ")[0].trim();
        String rawIdentifier = identifier.contains(":") ? identifier.split(":")[1] : identifier;

        Map<String, Object> rtn = new HashMap<>();
        rtn.put("dbId", ++maxDbId);

        String gn = interactor.getAliasWithoutSpecies(false);
        if (gn != null && !gn.isEmpty()) {
            String[] geneName = new String[1];
            geneName[0] = gn;
            rtn.put("geneName", geneName);
            rtn.put("displayName", identifier + " " + gn);    //Unified to Reactome name
        } else {
            rtn.put("displayName", identifier);               //Unified to Reactome name
        }

        Class schemaClass;
        Long refDbId;
        if (resource.getName().toLowerCase().contains("uniprot")) {
            refDbId = REACTOME_UNIPROT_REFERENCE_DATABASE;
            //displayName added below
            rtn.put("identifier", rawIdentifier.split("-")[0]);  //DO NOT MOVE OUTSIDE
            rtn.put("databaseName", "UniProt");

            rtn.put("url", "http://www.uniprot.org/entry/" + rawIdentifier);
            if (rawIdentifier.contains("-")) {
                rtn.put("variantIdentifier", rawIdentifier);
                //isofromParent //TODO
                schemaClass = ReferenceIsoform.class;
            } else {
                schemaClass = ReferenceGeneProduct.class;
            }
        } else if (resource.getName().toLowerCase().contains("chebi")) {
            refDbId = REACTOME_CHEBI_REFERENCE_DATABASE;
            //displayName added below
            rtn.put("identifier", rawIdentifier);  //DO NOT MOVE OUTSIDE
            String alias = interactor.getAlias();
            if(alias != null && !alias.isEmpty()) {
                String[] name = new String[1];
                name[0] = alias;
                rtn.put("name", name);
            }
            rtn.put("databaseName", resource.getName());
            rtn.put("url", "https://www.ebi.ac.uk/chebi/searchId.do?chebiId=CHEBI:" + rawIdentifier);
            schemaClass = ReferenceMolecule.class;
        } else {
            resource.setName("IntAct");
            refDbId = intActReferenceDatabaseDbId;
            rtn.put("identifier", rawIdentifier);  //DO NOT MOVE OUTSIDE
            rtn.put("databaseName", resource.getName());
            rtn.put("url", "https://www.ebi.ac.uk/intact/query/" + rawIdentifier);
            schemaClass = ReferenceGeneProduct.class;
        }
        if (interactor.getSynonyms() != null && !interactor.getSynonyms().isEmpty()) {
            rtn.put("secondaryIdentifier", interactor.getSynonyms().split("\\$"));
        }
        rtn.put("schemaClass", schemaClass.getSimpleName());

        //These two will removed from the map
        rtn.put("referenceDatabaseNode", dbIds.get(refDbId));
        rtn.put("labels", ReactomeBatchImporter.getLabels(schemaClass));

        return rtn;
    }

    private void initialise() {
        try {
            System.out.print("\n\nCleaning instances cache...");
            importLogger.info("Cleaning instances cache");
            dba.refresh();
            if (useUserInteractionData) {
                System.out.print("\rConnecting to the provided interaction data...");
                importLogger.info("Connecting to the provided interaction data");
                interactorsDatabase = new InteractorsDatabase(userInteractionDataFile);
                importLogger.info("Connected to the provided interaction data");
                System.out.print("\rConnected to the provided interaction data");
            } else {
                System.out.print("\rRetrieving interaction data...");
                importLogger.info("Retrieving interaction data");
                interactorsDatabase = IntactParser.getInteractors(INTERACTION_DATA_TMP_FILE);
                importLogger.info("Interaction data retrieved");
                System.out.print("\rInteraction data retrieved");
            }
            interactionService = new InteractionService(interactorsDatabase);
            interactorResourceService = new InteractorResourceService(interactorsDatabase);
        } catch (SQLException | IOException e) {
            System.out.println("\rAn error occurred while retrieving the interaction data");
            importLogger.error("An error occurred while retrieving the interaction data", e);
        } catch (Exception e) {
            importLogger.error("An error occurred while cleaning instances cache", e);
        }
    }

    //It seems like the best way of cleaning the cache is to close the connection and connect again
    private void cleanInteractorsCache(){
        try {
            importLogger.trace("Cleaning interactors cache");
            interactorsDatabase.getConnection().close();
            interactorsDatabase = new InteractorsDatabase(useUserInteractionData ? userInteractionDataFile : INTERACTION_DATA_TMP_FILE);
            interactionService = new InteractionService(interactorsDatabase);
            interactorResourceService = new InteractorResourceService(interactorsDatabase);
            importLogger.trace("Interactors cache cleaned");
        } catch (SQLException e) {
            importLogger.error("An error occurred while reconnecting to the interaction database", e);
        }
    }

    private void finalise() {
        try {
            interactorsDatabase.getConnection().close();
        } catch (SQLException e) {
            importLogger.error(e.getMessage(), e);
        }
        FileUtils.deleteQuietly(new File(INTERACTION_DATA_TMP_FILE));
    }

    /**
     * Returns a list of ReferenceEntity instances that are target for interaction data and at the same
     * time populates the referenceEntityMap with all the (identifier->[ReferenceEntity instance dbId])
     * contained in the database
     *
     * @return a list of ReferenceEntity instances that are target for interaction data
     */
    private Collection<GKInstance> getTargetReferenceEntities() {
        System.out.print("\rRetrieving interaction data target ReferenceEntity instances...");
        importLogger.info("Retrieving target ReferenceEntity instances");
        Collection<GKInstance> rtn = new HashSet<>();
        try {
            for (Object reAux : dba.fetchInstancesByClass(ReactomeJavaConstants.ReferenceEntity)) {
                GKInstance re = (GKInstance) reAux;
                if (re != null) {
                    String identifier = null;
                    if (re.getSchemClass().isValidAttribute(ReactomeJavaConstants.variantIdentifier)) {
                        identifier = (String) re.getAttributeValue(ReactomeJavaConstants.variantIdentifier);
                    }
                    if (identifier == null && re.getSchemClass().isValidAttribute(ReactomeJavaConstants.identifier)) {
                        identifier = (String) re.getAttributeValue(ReactomeJavaConstants.identifier);
                    }

                    boolean deflateRE = true;
                    if (identifier != null) {
                        GKInstance refDB = (GKInstance) re.getAttributeValue(ReactomeJavaConstants.referenceDatabase);
                        identifier = refDB.getDisplayName() + ":" + identifier;

                        referenceEntityMap.computeIfAbsent(identifier, k -> new HashSet<>()).add(re.getDBID());

                        Collection pes = re.getReferers(ReactomeJavaConstants.referenceEntity);
                        if (pes != null) {
                            for (Object peAux : pes) {
                                GKInstance pe = (GKInstance) peAux;
                                if (isTarget(pe)) {
                                    rtn.add(re);
                                    deflateRE = false;
                                    break;  //Only one of the referral PhysicalEntities has to be target for re to be added
                                }
                            }
                        }
                    }
                    if (deflateRE) re.deflate();
                }
            }
        } catch (Exception e) {
            importLogger.error("An error occurred while retrieving the target ReferenceEntity instances", e);
        }
        System.out.print("\rRetrieving interaction data target ReferenceEntity instances >> Done");
        importLogger.info("Target ReferenceEntity instances retrieved");
        return rtn;
    }

    private boolean isTarget(GKInstance pe) {
        boolean rtn = false;
        try {
            rtn = pe.getReferers(ReactomeJavaConstants.input) != null ||
                  pe.getReferers(ReactomeJavaConstants.output) != null ||
                  pe.getReferers(ReactomeJavaConstants.physicalEntity) != null ||
                  pe.getReferers(ReactomeJavaConstants.regulator) != null;
        } catch (Exception e) {
            /*nothing here*/
        } finally {
            pe.deflate();
        }
        return rtn;
    }

    private String getReferenceDatabaseName(GKInstance referenceEntity){
        try {
            GKInstance refDatabase = (GKInstance) referenceEntity.getAttributeValue(ReactomeJavaConstants.referenceDatabase);
            return refDatabase.getDisplayName();
        } catch (Exception e) {
            return "undefined";
        }
    }

    private List<Interaction> getIntActInteraction(String resource, String identifier){
        try {
            String target = resource + ":" + identifier;
            return interactionService.getInteractions(target, "static");
        } catch (InvalidInteractionResourceException | SQLException e) {
            return new ArrayList<>();
        }
    }

    private static Map<Long, InteractorResource> interactorResourceMap = new HashMap<>();

    private InteractorResource getInteractorResource(Interactor interactor){
        InteractorResource ir = interactorResourceMap.get(interactor.getInteractorResourceId());
        if(ir == null) {
            try {
                ir = interactorResourceService.getAllMappedById().get(interactor.getInteractorResourceId());
                interactorResourceMap.put(interactor.getInteractorResourceId(), ir);
            } catch (SQLException e) {
                //Nothing here
            }
        }
        return ir;
    }

    private void updateProgressBar(long done, long total) {
        if(done == total || (done > 0 && done % 10 == 0)) {
            String format = "\rInteraction data import: %3d%% %s %c";
            char[] rotators = {'|', '/', '—', '\\'};
            double percent = (double) done / total;
            StringBuilder progress = new StringBuilder(PROGRESS_BAR_WITH);
            progress.append('|');
            int i = 0;
            for (; i < (int) (percent * PROGRESS_BAR_WITH); i++) progress.append("=");
            for (; i < PROGRESS_BAR_WITH; i++) progress.append(" ");
            progress.append('|');
            System.out.printf(format, (int) (percent * 100), progress, rotators[(int) ((done - 1) % (rotators.length * 10)) / 10]);
        }
    }
}
