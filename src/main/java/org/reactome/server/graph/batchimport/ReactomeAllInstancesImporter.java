package org.reactome.server.graph.batchimport;

import static org.reactome.server.graph.utils.FormatUtils.getTimeFormatted;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.IllegalClassException;
import org.apache.commons.lang.StringUtils;
import org.gk.model.GKInstance;
import org.gk.model.ReactomeJavaConstants;
import org.gk.pathwaylayout.PathwayDiagramXMLGenerator;
import org.gk.persistence.MySQLAdaptor;
import org.gk.schema.InvalidClassException;
import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.io.layout.DatabaseLayout;
import org.reactome.server.graph.Main;
import org.reactome.server.graph.domain.annotations.ReactomeProperty;
import org.reactome.server.graph.domain.annotations.ReactomeRelationship;
import org.reactome.server.graph.domain.annotations.ReactomeTransient;
import org.reactome.server.graph.domain.model.Complex;
import org.reactome.server.graph.domain.model.DatabaseObject;
import org.reactome.server.graph.domain.model.EntitySet;
import org.reactome.server.graph.domain.model.Event;
import org.reactome.server.graph.domain.model.ExternalOntology;
import org.reactome.server.graph.domain.model.GO_Term;
import org.reactome.server.graph.domain.model.GenomeEncodedEntity;
import org.reactome.server.graph.domain.model.LiteratureReference;
import org.reactome.server.graph.domain.model.Pathway;
import org.reactome.server.graph.domain.model.Person;
import org.reactome.server.graph.domain.model.PhysicalEntity;
import org.reactome.server.graph.domain.model.Reaction;
import org.reactome.server.graph.domain.model.ReactionLikeEvent;
import org.reactome.server.graph.domain.model.ReferenceEntity;
import org.reactome.server.graph.domain.model.ReferenceIsoform;
import org.reactome.server.graph.domain.model.Species;
import org.reactome.server.graph.domain.model.Taxon;
import org.reactome.server.graph.utils.GKInstanceHelper;
import org.reactome.server.graph.utils.ProgressBarUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.neo4j.core.schema.Relationship;

/**
 * Directly copied ReactomeBatchImporter and then modified to import all instances from Reactome's MySQL database.
 * Since we will do this only once, there is no need to keep the code clean and modular for now at least (NB by GW).
 */
public class ReactomeAllInstancesImporter {

    private static final Logger importLogger = LoggerFactory.getLogger("import");
    private static final Logger errorLogger = LoggerFactory.getLogger("import_error");
    private static final Logger consistencyCheckSummaryLogger = LoggerFactory.getLogger("consistency_check_summary");
    private static final Logger consistencyCheckReportLogger = LoggerFactory.getLogger("consistency_check_report");

    private static MySQLAdaptor dba;
    private static BatchInserter batchInserter;
    private static String DATA_DIR;
    private String neo4jVersion;

    private static final String DBID = "dbId";
    private static final String STID = "stId";
    private static final String DELETED_STID = "deletedStId";
    private static final String OLD_STID = "oldStId";
    private static final Long TAXONOMY_ROOT = 164487L;
    private static final String TAXONOMY_ID = "taxId";
    private static final String IDENTIFIER = "identifier";
    private static final String VARIANT_IDENTIFIER = "variantIdentifier";
    private static final String NAME = "displayName";

    public static final String STOICHIOMETRY = "stoichiometry";
    public static final String ORDER = "order";

    private static final Map<Class<?>, List<ReactomeAttribute>> primitiveAttributesMap = new HashMap<>();
    private static final Map<Class<?>, List<ReactomeAttribute>> primitiveListAttributesMap = new HashMap<>();
    private static final Map<Class<?>, List<ReactomeAttribute>> relationAttributesMap = new HashMap<>();
    private static final Map<ReactomeAttribute, String> attributeRenaming = new HashMap<>();

    public static Long maxDbId;
    private static final Map<Long, Long> dbIds = new HashMap<>();
    // No instances will be discarded. Therefore, this set is not used.
//    private static final Set<Long> discarded = new HashSet<>();
    private static final Map<Long, Long> reverseReactions = new HashMap<>();
    private static final Map<Long, Long> equivalentTo = new HashMap<>();
    private static final Map<Class<?>, Label[]> labelMap = new HashMap<>();
    private static final Map<Integer, Long> taxIdDbId = new HashMap<>();

    private static final Set<Long> topLevelPathways = new HashSet<>();

    private static int total;

    private static final DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private Set<String> trivialMolecules;
    private final GKInstanceHelper gkInstanceHelper;

    public ReactomeAllInstancesImporter(String host, Integer port, String name, String user, String password, String neo4j,
                                 boolean includeInteractors, String interactorsFile, boolean isSQLLite, String neo4jVersion) {
        try {
            DATA_DIR = neo4j;
            this.neo4jVersion = neo4jVersion;
            dba = new MySQLAdaptor(host, name, user, password, port);
            maxDbId = dba.fetchMaxDbId();

            total = (int) dba.getClassInstanceCount(ReactomeJavaConstants.DatabaseObject);
            total = total - (int) dba.getClassInstanceCount(ReactomeJavaConstants.FrontPage);
//            total = total - (int) dba.getClassInstanceCount(ReactomeJavaConstants.PathwayDiagramItem);
//            total = total - (int) dba.getClassInstanceCount(ReactomeJavaConstants.ReactionCoordinates);
//            total = total - (int) dba.getClassInstanceCount(ReactomeJavaConstants._Release);
            importLogger.info("Established connection to Reactome database");
        } catch (SQLException | InvalidClassException e) {
            importLogger.error("An error occurred while connection to the Reactome database", e);
        }

        //Getting the trivial molecules list in order to enrich the ReferenceMolecule objects while creating the graph
        try {
            System.out.print("Retrieving the trivial molecules list...");
            importLogger.info("Retrieving the trivial molecules list");
            trivialMolecules = new HashSet<>();
            ClassLoader loader = Main.class.getClassLoader();
            //noinspection ConstantConditions
            for (String line : IOUtils.toString(loader.getResourceAsStream("trivialMolecules.txt"), Charset.defaultCharset()).split("\n")) {
                trivialMolecules.add(line.split("\t")[0]);
            }
            importLogger.info("Trivial molecules list successfully retrieved");
            System.out.println("\rTrivial molecules list successfully retrieved.");
        } catch (IOException e) {
            importLogger.error("An error occurred while retrieving the trivial molecules", e);
        }

        gkInstanceHelper = new GKInstanceHelper(dba);
    }

    @SuppressWarnings("unchecked")
    public void importAll(boolean barComplete) throws IOException {
        final long start = System.currentTimeMillis();
        prepareDatabase();

        try {
            // We don't need top level pathways here. But we still need to set the top level pathways up so that
            // they can be labeled as TopLevelPathway in the graph.
            List<GKInstance> topLevelPathways = this.getTopLevelPathways();
            importLogger.info("Top level pathways retrieved: " + topLevelPathways.size());
            
            Collection<GKInstance> allInstances = dba.fetchInstancesByClass(ReactomeJavaConstants.DatabaseObject);
            for (GKInstance instance : allInstances) {
                if (instance.getSchemClass() == null) {
                    // This instance has no schema class, so it cannot be imported
                    errorLogger.error("Instance with dbId: " + instance.getDBID() + " has no schema class. Skipping import.");
                    System.err.println("Instance with dbId: " + instance.getDBID() + " has no schema class. Skipping import.");
                    continue;
                }
                if (instance.getSchemClass().isa(ReactomeJavaConstants.FrontPage)) {
                    // FrontPage is not needed any more in the graph database. TopLevelPathways will be used instead.
                    continue;
                }
                if (dbIds.containsKey(instance.getDBID())) {
                    // This instance has already been imported
                    continue;
                }
                importGkInstance(instance);
            }
                    
            importLogger.info("All instances imported successfully");
            System.out.println("All instances imported successfully: " + allInstances.size() + " instances");
        } 
        catch (Exception e) {
            e.printStackTrace();
        }

        printConsistencyCheckReport();

        importLogger.info("Storing the graph");
        System.out.print("\n\nPlease wait while storing the graph...");
        batchInserter.shutdown();
        importLogger.info("The database '" + dba.getDBName() + "' has been imported to Neo4j");
        Long time = System.currentTimeMillis() - start;
        System.out.println("\rThe database '" + dba.getDBName() + "' has been imported to Neo4j (" + getTimeFormatted(time) + ")");

    }


    private List<GKInstance> getTopLevelPathways() throws Exception {
        Collection<?> frontPages = dba.fetchInstancesByClass(ReactomeJavaConstants.FrontPage);
        GKInstance frontPage = (GKInstance) frontPages.iterator().next();
        Collection<?> objects = frontPage.getAttributeValuesList(ReactomeJavaConstants.frontPageItem);
        List<GKInstance> tlps = new ArrayList<>();
        for (Object object : objects) {
            GKInstance instance = (GKInstance) object;
            topLevelPathways.add(instance.getDBID());
            Collection<?> orthologousEvents = getCollectionFromGkInstance(instance, ReactomeJavaConstants.orthologousEvent);
            if (orthologousEvents != null && !orthologousEvents.isEmpty()) {
                for (Object obj : orthologousEvents) {
                    GKInstance orthologousEvent = (GKInstance) obj;
                    topLevelPathways.add(orthologousEvent.getDBID());
                }
            }
            tlps.add(instance);
        }
        return tlps;
    }

    private List<GKInstance> getInstancesByClass(String className) throws Exception {
        Collection<?> instances = dba.fetchInstancesByClass(className);
        return instances.stream().map(o -> (GKInstance) o).collect(Collectors.toList());
    }

    private GKInstance getLatestRelease() throws Exception {
        Collection<?> releases = dba.fetchInstancesByClass("_Release");
        return releases.stream().map(o -> ((GKInstance) o)).max(Comparator.comparing(instance -> {
            try {
                return (int) instance.getAttributeValue("releaseNumber");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        })).orElse((GKInstance) releases.iterator().next());
    }

    /**
     * Imports one single GkInstance into neo4j.
     * When iterating through the relationAttributes it is possible to go deeper
     * into the GkInstance hierarchy (eg hasEvents)
     *
     * @param instance GkInstance
     * @return Neo4j native id (generated by the BatchInserter)
     */
    private Long importGkInstance(GKInstance instance) throws ClassNotFoundException {
//        ProgressBarUtils.updateProgressBar(dbIds.size() + discarded.size(), total);
        ProgressBarUtils.updateProgressBar(dbIds.size(), total);

        String clazzName = getClassName(instance);
        Class<?> clazz = Class.forName(clazzName);
        setUpFields(clazz); //Sets up the attribute map per class populating relationAttributesMap and primitiveListAttributesMap
        Long id = saveDatabaseObject(instance, clazz);
        dbIds.put(instance.getDBID(), id); //caching the "saved" object mapped to the corresponding Neo4j node id

        if (relationAttributesMap.containsKey(clazz)) {
            for (ReactomeAttribute reactomeAttribute : relationAttributesMap.get(clazz)) {
                String targetAttribute = reactomeAttribute.getAttribute();
                String originAttribute = attributeRenaming.getOrDefault(reactomeAttribute, targetAttribute);
                ReactomeAttribute.PropertyType type = reactomeAttribute.getType();
                switch (originAttribute) {
                    case "orthologousEvent":
                        if (isCuratedEvent(instance)) {
                            Collection<GKInstance> orthologousAll = getCollectionFromGkInstance(instance, originAttribute);
                            if (orthologousAll != null && !orthologousAll.isEmpty()) {
                                Collection<?> alreadyPointing = getCollectionFromGkInstanceReferrals(instance, ReactomeJavaConstants.inferredFrom);
                                //orthologousEvents collection will only contain those that are not pointing to instance as inferredFrom to avoid inferredTo duplicates
                                Collection<GKInstance> orthologousEvents = new ArrayList<>();
                                for (GKInstance orthologousEvent : orthologousAll) {
                                    if (isGKInstanceInCollection(orthologousEvent, alreadyPointing)) {
                                        if (!dbIds.containsKey(orthologousEvent.getDBID())) {
                                            //The link is not added but it has to be imported to ensure the object (and link) are created
                                            importGkInstance(orthologousEvent);
                                        }
                                    } else {
                                        orthologousEvents.add(orthologousEvent);
                                    }
                                }
                                //saveRelationships might enter in recursion so changes in "orthologousEvent" have to be carefully thought
                                saveRelationships(id, orthologousEvents, "inferredTo");
                            }
                        }
                        break;
                    case "inferredFrom": //Only for Event because in PhysicalEntity is ReactomeTransient
                        if (isValidGkInstanceAttribute(instance, originAttribute)) {
                            Collection<GKInstance> inferredFrom = getCollectionFromGkInstance(instance, originAttribute);
                            saveRelationships(id, inferredFrom, "inferredToReverse");
                        }
                        break;
                    case "inferredTo": //Only for PhysicalEntity
                        if (isValidGkInstanceAttribute(instance, originAttribute)) {
                            Collection<GKInstance> inferredTo = getCollectionFromGkInstance(instance, originAttribute);
                            if (inferredTo == null || inferredTo.isEmpty()) {
                                inferredTo = getCollectionFromGkInstanceReferrals(instance, ReactomeJavaConstants.inferredFrom);
                            }
                            saveRelationships(id, inferredTo, targetAttribute);
                        }
                        break;
                    case "hasEncapsulatedEvent":
                        // Disable this. 
//                        GKInstance normalPathway = (GKInstance) getObjectFromGkInstance(instance, ReactomeJavaConstants.normalPathway);
//                        if (normalPathway == null) { //No encapsulation is taken into account for none infectious disease pathways
//                            try {
//                                GKInstance diagram = gkInstanceHelper.getHasDiagram(instance);
//                                if (diagram != null) {
//                                    PathwayDiagramXMLGenerator xmlGenerator = new PathwayDiagramXMLGenerator();
//                                    String xml = xmlGenerator.generateXMLForPathwayDiagram(diagram, instance);
//                                    Collection<GKInstance> encapsulatedEvents = new HashSet<>();
//                                    for (String line : StringUtils.split(xml, System.lineSeparator())) {
//
//                                        if (line.trim().startsWith("<org.gk.render.ProcessNode") && line.contains("reactomeId")) {
//                                            String dbId = line.split("reactomeId=\"")[1].split("\"")[0];
//                                            GKInstance target = dba.fetchInstance(Long.valueOf(dbId));
//                                            if (!gkInstanceHelper.pathwayContainsProcessNode(instance, target)) {
//                                                encapsulatedEvents.add(target);
//                                            }
//                                        }
//                                    }
//                                    diagram.deflate();
//                                    saveRelationships(id, encapsulatedEvents, targetAttribute);
//                                }
//                            } catch (Exception e) {
//                                errorLogger.error("An exception occurred while trying to retrieve a diagram from entry with dbId: " + instance.getDBID() + "and name: " + instance.getDisplayName());
//                            }
//                        }
                        break;
                    case "modifiedList":
                        Collection<GKInstance> modifiedList = getCollectionFromGkInstance(instance, ReactomeJavaConstants.modified);
                        if (modifiedList != null && modifiedList.size() > 0) {
                            saveRelationships(id, modifiedList, targetAttribute);
                        }
                        break;
                    default:
                        if (isValidGkInstanceAttribute(instance, originAttribute)) {
                            Collection<GKInstance> relationships = getCollectionFromGkInstance(instance, originAttribute);
                            if (isConsistent(instance, relationships, originAttribute, type)) {
                                //saveRelationships might enter in recursion so changes in "orthologousEvent" have to be carefully thought
                                saveRelationships(id, relationships, targetAttribute);
                            }
                        }
                }
            }
        }
        instance.deflate(); //will ensure that the use of the GkInstance does not end in an OutOfMemory exception
        return id;
    }

    /**
     * Saves one single GkInstance to neo4j. Only primitive attributes will be saved (Attributes that are not reference
     * to another GkInstance eg values like Strings)
     * Get the attributes map and check null is slightly faster than contains.
     *
     * @param instance GkInstance
     * @param clazz    Clazz of object that will result form converting the instance (eg Pathway, Reaction)
     * @return Neo4j native id (generated by the BatchInserter)
     */
    private Long saveDatabaseObject(GKInstance instance, Class<?> clazz) throws IllegalArgumentException {

        Label[] labels = getLabels(clazz);
        String schemaClass = clazz.getSimpleName();
        if (topLevelPathways.contains(instance.getDBID())) {
            schemaClass = "TopLevelPathway";
            Label[] newLabels = Arrays.copyOf(labels, labels.length + 1);
            newLabels[labels.length] = Label.label(schemaClass);
            labels = newLabels;
        }

        /*
         * GKInstances getAttribute value method do NOT provide dbID nor displayName
         * We save those two the "hard" way
         */
        Map<String, Object> properties = new HashMap<>();
        properties.put("schemaClass", schemaClass);
        properties.put(DBID, instance.getDBID());
        if (instance.getDisplayName() != null) {
            // TO fix Different Styles in Person display Name (example Jupe, Steven or Jupe, S)
            if (instance.getSchemClass().isa(ReactomeJavaConstants.Person)) {
                String name = "";
                String initial = (String) getObjectFromGkInstance(instance, ReactomeJavaConstants.initial);
                if (initial != null) {
                    name = ", " + initial;
                } else {
                    String firstName = (String) getObjectFromGkInstance(instance, ReactomeJavaConstants.firstname);
                    if (firstName != null) name = ", " + firstName;
                }
                String surname = (String) getObjectFromGkInstance(instance, ReactomeJavaConstants.surname);
                properties.put(NAME, surname + name);
            } else {
                properties.put(NAME, instance.getDisplayName());
            }
        } else {
            //These guys will also be reported in one of the GraphQA tests in the graph-qa project
            errorLogger.error("Found an entry without display name! dbId: " + instance.getDBID());
        }

        // Next thing is iterating across all the primitive attributes previously mapped in primitiveAttributesMap
        if (primitiveAttributesMap.containsKey(clazz)) {
            for (ReactomeAttribute reactomeAttribute : primitiveAttributesMap.get(clazz)) {
                String targetAttribute = reactomeAttribute.getAttribute();
                String originAttribute = attributeRenaming.getOrDefault(reactomeAttribute, targetAttribute);
                ReactomeAttribute.PropertyType type = reactomeAttribute.getType();
                switch (originAttribute) {
                    case STID:
                        GKInstance stableIdentifier = (GKInstance) getObjectFromGkInstance(instance, ReactomeJavaConstants.stableIdentifier);
                        if (stableIdentifier == null) continue;
                        String stId = (String) getObjectFromGkInstance(stableIdentifier, ReactomeJavaConstants.identifier);
                        if (stId == null) continue;
                        properties.put(targetAttribute, stId);
                        //Stable identifier version
                        String version = (String) getObjectFromGkInstance(stableIdentifier, ReactomeJavaConstants.identifierVersion);
                        if (version != null) properties.put("stIdVersion", stId + "." + version);
                        //Keeping old stable identifier if present
                        String oldStId = (String) getObjectFromGkInstance(stableIdentifier, "oldIdentifier");
                        if (oldStId != null) {
                            if (oldStId.isEmpty()) { //Avoids adding empty OLD_STID in the graph database
                                errorLogger.warn("'" + OLD_STID + "' is empty for " + instance.getDBID() + ": " + instance.getDisplayName());
                            } else {
                                properties.put(OLD_STID, oldStId);
                            }
                        }
                        break;
                    case DELETED_STID:
                        GKInstance deletedStableIdentifier = (GKInstance) getObjectFromGkInstance(instance, "deletedStableIdentifier");
                        if (deletedStableIdentifier == null) continue;
                        String dStId = (String) getObjectFromGkInstance(deletedStableIdentifier, ReactomeJavaConstants.identifier);
                        if (dStId == null) continue;
                        properties.put(targetAttribute, dStId);
                        break;
                    case "orcidId":
                        GKInstance orcid = (GKInstance) getObjectFromGkInstance(instance, ReactomeJavaConstants.crossReference);
                        if (orcid == null) continue;
                        String orcidId = (String) getObjectFromGkInstance(orcid, ReactomeJavaConstants.identifier);
                        if (orcidId == null) continue;
                        properties.put(targetAttribute, orcidId);
                        break;
                    case TAXONOMY_ID:
                        GKInstance taxon = (GKInstance) getObjectFromGkInstance(instance, ReactomeJavaConstants.crossReference);
                        String taxId = taxon != null ? (String) getObjectFromGkInstance(taxon, ReactomeJavaConstants.identifier) : null;
                        if (taxId != null && !taxId.isEmpty()) {
                            taxIdDbId.put(Integer.valueOf(taxId), instance.getDBID());
                            properties.put(targetAttribute, taxId);
                        } else if (!instance.getDBID().equals(TAXONOMY_ROOT)) {
                            errorLogger.warn("'" + TAXONOMY_ID + "' cannot be set for " + instance.getDBID() + ": " + instance.getDisplayName());
                        }
                        break;
                    case "hasDiagram":
                        GKInstance diagram = gkInstanceHelper.getHasDiagram(instance);
                        boolean hasDiagram = diagram != null;
                        properties.put(targetAttribute, hasDiagram);
                        if (hasDiagram) {
                            properties.put("diagramWidth", getObjectFromGkInstance(diagram, "width"));
                            properties.put("diagramHeight", getObjectFromGkInstance(diagram, "height"));
                            diagram.deflate();
                        }
                        properties.put(targetAttribute, hasDiagram);
                        break;
                    case "hasEHLD":
                        Boolean hasEHLD = (Boolean) getObjectFromGkInstance(instance, ReactomeJavaConstants.hasEHLD);
                        properties.put(targetAttribute, hasEHLD != null && hasEHLD);
                        break;
                    case "isInDisease":
                        GKInstance disease = (GKInstance) getObjectFromGkInstance(instance, ReactomeJavaConstants.disease);
                        properties.put(targetAttribute, disease != null);
                        break;
                    case "isInferred":
                        GKInstance isInferredFrom = (GKInstance) getObjectFromGkInstance(instance, ReactomeJavaConstants.inferredFrom);
                        properties.put(targetAttribute, isInferredFrom != null);
                        break;
                    case "referenceType":
                        GKInstance referenceEntity = (GKInstance) getObjectFromGkInstance(instance, ReactomeJavaConstants.referenceEntity);
                        if (referenceEntity == null) continue;
                        properties.put(targetAttribute, referenceEntity.getSchemClass().getName());
                        break;
                    case "speciesName":
                        if (instance.getSchemClass().isa(ReactomeJavaConstants.OtherEntity)) continue;
                        if (instance.getSchemClass().isa(ReactomeJavaConstants.ChemicalDrug)) continue;
                        List<?> speciesList = (List<?>) getCollectionFromGkInstance(instance, ReactomeJavaConstants.species);
                        if (speciesList == null || speciesList.isEmpty()) continue;
                        GKInstance species = (GKInstance) speciesList.get(0);
                        properties.put(targetAttribute, species.getDisplayName());
                        break;
                    case "trivial":
                        String chebiId = (String) getObjectFromGkInstance(instance, "identifier");
                        properties.put(targetAttribute, chebiId != null && trivialMolecules.contains(chebiId));
                        break;
                    case "url": //Can be added or existing
                        if (!instance.getSchemClass().isa(ReactomeJavaConstants.ReferenceDatabase) && !instance.getSchemClass().isa(ReactomeJavaConstants.Figure)) {
                            GKInstance referenceDatabase = (GKInstance) getObjectFromGkInstance(instance, ReactomeJavaConstants.referenceDatabase);
                            if (referenceDatabase == null) continue;
                            String databaseName = referenceDatabase.getDisplayName();
                            String identifier;
                            if (instance.getSchemClass().isa(ReactomeJavaConstants.GO_BiologicalProcess) || instance.getSchemClass().isa(ReactomeJavaConstants.GO_MolecularFunction) || instance.getSchemClass().isa(ReactomeJavaConstants.GO_CellularComponent)) {
                                identifier = (String) getObjectFromGkInstance(instance, ReactomeJavaConstants.accession);
                            } else {
                                if (instance.getSchemClass().isa(ReactomeJavaConstants.ReferenceIsoform)) {
                                    identifier = (String) getObjectFromGkInstance(instance, ReactomeJavaConstants.variantIdentifier);
                                } else {
                                    identifier = (String) getObjectFromGkInstance(instance, ReactomeJavaConstants.identifier);
                                }
                            }
                            String url = (String) getObjectFromGkInstance(referenceDatabase, ReactomeJavaConstants.accessUrl);
                            if (url == null || identifier == null) continue;

                            properties.put("databaseName", databaseName);
                            properties.put(targetAttribute, url.replace("###ID###", identifier));
                        } else {
                            defaultAction(instance, originAttribute, targetAttribute, properties, type);   //Can be added or existing
                        }
                        break;
                    default: //Here we are in a none graph-added field. The field content has to be treated based on the schema definition
                        defaultAction(instance, originAttribute, targetAttribute, properties, type);
                }
            }
        }

        // Last thing is iterating across all the list of objects previously mapped in primitiveListAttributesMap
        if (primitiveListAttributesMap.containsKey(clazz)) {
            for (ReactomeAttribute reactomeAttribute : primitiveListAttributesMap.get(clazz)) {
                String targetAttribute = reactomeAttribute.getAttribute();
                String originAttribute = attributeRenaming.getOrDefault(reactomeAttribute, targetAttribute);
                ReactomeAttribute.PropertyType type = reactomeAttribute.getType();
                if (isValidGkInstanceAttribute(instance, originAttribute)) {
                    Collection<?> values = getCollectionFromGkInstance(instance, originAttribute);
                    if (isConsistent(instance, values, originAttribute, type)) {
                        Class<?> attributeType = reactomeAttribute.getClazz();
                        Object[] castedValues = (Object[]) Array.newInstance(attributeType, values.size());
                        values.stream().map(attributeType::cast).collect(Collectors.toList()).toArray(castedValues);
                        properties.put(targetAttribute, castedValues);
                    }
                }
            }
        }

        // The node is now ready to be inserted in the graph database
        try {
            return batchInserter.createNode(properties, labels);
        } catch (IllegalArgumentException e) {
            throw new IllegalClassException("A problem occurred when trying to save entry to the Graph: " + instance.getDisplayName() + ":" + instance.getDBID());
        }
    }

    //saveDatabaseObject default option
    private void defaultAction(GKInstance instance, String originAttribute, String targetAttribute, Map<String, Object> properties, ReactomeAttribute.PropertyType type) {
        if (isValidGkInstanceAttribute(instance, originAttribute)) {
            Object value = getObjectFromGkInstance(instance, originAttribute);
            if (isConsistent(instance, value, originAttribute, type)) {
                properties.put(targetAttribute, value);
            }
        }
    }

    /**
     * Creating a relationship between the old instance (using oldId) and its children (List objects).
     * Relationships will be created depth first, if new instance does not already exist recursion will begin
     * (newId = importGkInstance)
     * Every relationship entry will have a stoichiometry attribute, which is used as a counter. The same input of a Reaction
     * for example can be present multiple times. Instead of saving a lot of relationships we just set a counter to indicate
     * this behaviour. Since we can not query using the Batch inserter we have to iterate the collection of relationships
     * first to identify the duplicates.
     * The stoichiometry map has to utilize a helperObject because the GkInstance does not implement Comparable and
     * comparing instances will not work. In the helperObject the instance and a counter will be saved. Counter is used
     * to set stoichiometry of a relationship.
     *
     * @param oldId        Old native neo4j id, used for saving a relationship to neo4j.
     * @param objects      New list of GkInstances that have relationship to the old Instance (oldId).
     * @param relationName Name of the relationship.
     */
    private void saveRelationships(Long oldId, Collection<GKInstance> objects, String relationName) throws ClassNotFoundException {
        if (objects == null || objects.isEmpty()) return;

        if (relationName.equals("modified")) {
            try {
                GKInstance latestModified = objects.iterator().next();
                Date latestDate = formatter.parse((String) getObjectFromGkInstance(latestModified, ReactomeJavaConstants.dateTime));
                for (Object object : objects) {
                    GKInstance gkInstance = (GKInstance) object;
                    //All go to discarded and the chosen one will be removed
//                    if (!dbIds.containsKey(gkInstance.getDBID())) 
//                        discarded.add(gkInstance.getDBID());
                    Date date = formatter.parse((String) getObjectFromGkInstance(gkInstance, ReactomeJavaConstants.dateTime));
                    if (latestDate.before(date)) {
                        latestDate = date;
                        latestModified = gkInstance;
                    }
                }
                // We'd like to keep the original modified list. Copy it to avoid modifying the original.
                objects = Collections.singleton(latestModified);
//                objects.clear();
//                objects.add(latestModified);
//                discarded.remove(latestModified.getDBID());
            } 
            catch (Exception e) {
                importLogger.error("Problem while filtering tbe 'modified' relationship for " + oldId, e);
            }
        }

        Map<Long, GkInstancePropertiesHelper> propertiesMap = new HashMap<>();
        // Have to make sure that the objects are not null and have a schema class
        objects.stream().filter(Objects::nonNull).filter(inst -> inst.getSchemClass() != null).forEach(object -> {
            if (propertiesMap.containsKey(object.getDBID())) {
                propertiesMap.get(object.getDBID()).increment();
            } else {
                int order = propertiesMap.keySet().size();
                propertiesMap.put(object.getDBID(), new GkInstancePropertiesHelper(object, order));
            }
        });

        for (Long dbId : propertiesMap.keySet()) {
            GKInstance instance = propertiesMap.get(dbId).getInstance();
            Long newId;
            if (!dbIds.containsKey(dbId)) {
                newId = importGkInstance(instance);
                instance.deflate();
            } else {
                newId = dbIds.get(dbId);
            }
            Map<String, Object> properties = new HashMap<>();
            properties.put(STOICHIOMETRY, propertiesMap.get(dbId).getCount());
            properties.put(ORDER, propertiesMap.get(dbId).getOrder());
            RelationshipType relationshipType = RelationshipType.withName(relationName);
            saveRelationship(newId, oldId, relationshipType, properties);
        }
    }

    private void saveRelationship(Long toId, Long fromId, RelationshipType relationshipType, Map<String, Object> properties) {
        String relationName = relationshipType.name();
        switch (relationName) {
            case "reverseReaction":
                if (!(reverseReactions.containsKey(fromId) && reverseReactions.containsValue(toId)) &&
                        !(reverseReactions.containsKey(toId) && reverseReactions.containsValue(fromId))) {
                    batchInserter.createRelationship(fromId, toId, relationshipType, properties);
                    reverseReactions.put(fromId, toId);
                }
                break;
            case "equivalentTo":
                if (!(equivalentTo.containsKey(fromId) && equivalentTo.containsValue(toId)) &&
                        !(equivalentTo.containsKey(toId) && equivalentTo.containsValue(fromId))) {
                    batchInserter.createRelationship(fromId, toId, relationshipType, properties);
                    equivalentTo.put(fromId, toId);
                }
                break;
            case "inferredToReverse":
                batchInserter.createRelationship(toId, fromId, RelationshipType.withName("inferredTo"), properties);
                break;
            case "author":
            case "authored":
            case "created":
            case "edited":
            case "modified":
            case "revised":
            case "reviewed":
            case "modifiedList":      // These relationships are always created in the reverse direction
                batchInserter.createRelationship(toId, fromId, relationshipType, properties);
                break;
            default:
                batchInserter.createRelationship(fromId, toId, relationshipType, properties);
                break;
        }
    }

    /**
     * Cleaning the old database folder, instantiate BatchInserter, create Constraints for the new DB
     */
    private void prepareDatabase() throws IOException {
        File file = cleanDatabase();

        batchInserter = BatchInserters.inserter(DatabaseLayout.ofFlat(file.toPath()));
        createConstraints();
    }

    /**
     * Creating uniqueness constraints and indexes for the new DB.
     * WARNING: Constraints can not be enforced while importing, only after batchInserter.shutdown()
     */
    private void createConstraints() {

        createSchemaConstraint(DatabaseObject.class, DBID);
        createSchemaConstraint(DatabaseObject.class, STID);

        createSchemaConstraint(DatabaseObject.class, OLD_STID);
        //createDeferredSchemaIndex(DatabaseObject.class, OLD_STID); //Alternative to the previous one in case of duplicates

        createSchemaConstraint(Event.class, DBID);
        createSchemaConstraint(Event.class, STID);

        createSchemaConstraint(Pathway.class, DBID);
        createSchemaConstraint(Pathway.class, STID);

        createSchemaConstraint(ReactionLikeEvent.class, DBID);
        createSchemaConstraint(ReactionLikeEvent.class, STID);

        createSchemaConstraint(Reaction.class, DBID);
        createSchemaConstraint(Reaction.class, STID);

        createSchemaConstraint(PhysicalEntity.class, DBID);
        createSchemaConstraint(PhysicalEntity.class, STID);

        createSchemaConstraint(Complex.class, DBID);
        createSchemaConstraint(Complex.class, STID);

        createSchemaConstraint(EntitySet.class, DBID);
        createSchemaConstraint(EntitySet.class, STID);

        createSchemaConstraint(GenomeEncodedEntity.class, DBID);
        createSchemaConstraint(GenomeEncodedEntity.class, STID);

        createSchemaConstraint(ReferenceEntity.class, DBID);
        createSchemaConstraint(ReferenceEntity.class, STID);

        createSchemaConstraint(Taxon.class, TAXONOMY_ID);
        createSchemaConstraint(Species.class, TAXONOMY_ID);

        //Since the same author might be several times, due to the merging of different Reactome
        //databases in the past, there cannot be a schema constrain but an index over this field
        createDeferredSchemaIndex(Person.class, "orcidId");

        createDeferredSchemaIndex(LiteratureReference.class, "pubMedIdentifier");
        createDeferredSchemaIndex(ReferenceEntity.class, IDENTIFIER);
        createDeferredSchemaIndex(ReferenceEntity.class, VARIANT_IDENTIFIER);

        //This is needed
        createDeferredSchemaIndex(ReferenceIsoform.class, IDENTIFIER);
        createDeferredSchemaIndex(ReferenceIsoform.class, VARIANT_IDENTIFIER);
        createDeferredSchemaIndex(ExternalOntology.class, IDENTIFIER); // Needed by search-indexer
        createDeferredSchemaIndex(Taxon.class, NAME); // Needed by search-indexer
    }

    /**
     * Simple wrapper for creating a isUnique constraint
     *
     * @param clazz specific Class
     * @param name  fieldName
     */
    private static void createSchemaConstraint(Class<?> clazz, String name) {
        try {
            batchInserter.createDeferredConstraint(Label.label(clazz.getSimpleName())).assertPropertyIsUnique(name).create();
        } catch (Throwable e) {
            //ConstraintViolationException and PreexistingIndexEntryConflictException are both catch here
            importLogger.warn("Could not create Constraint on " + clazz.getSimpleName() + " for " + name);
        }
    }

    /**
     * Simple wrapper for creating an index
     *
     * @param clazz specific Class
     * @param name  fieldName
     */
    private static void createDeferredSchemaIndex(Class<?> clazz, String name) {
        try {
            batchInserter.createDeferredSchemaIndex(Label.label(clazz.getSimpleName())).on(name).create();
        } catch (Throwable e) {
            //ConstraintViolationException and PreexistingIndexEntryConflictException are both catch here
            importLogger.warn("Could not create Index on " + clazz.getSimpleName() + " for " + name);
        }
    }

    /**
     * Cleaning the Neo4j data import directory
     */
    private File cleanDatabase() {

        File dir = new File(DATA_DIR);
        try {
            if (dir.exists()) {
                FileUtils.cleanDirectory(dir);
            } else {
                FileUtils.forceMkdir(dir);
            }
        } catch (IOException | IllegalArgumentException e) {
            importLogger.warn("An error occurred while cleaning the old database");
        }
        return dir;
    }

    /**
     * For cases like the "Drug" instances, the graph database will keep different subclasses (e.g. ChemicalDrug or
     * ProteinDrug) instead of using the drugType as it is annotated in GK_Central
     *
     * @param instance the instance for which the class name is required
     * @return a String with the className to be assigned to the instance conversion
     */
    private static String getClassName(GKInstance instance) {
        String name = instance.getSchemClass().getName();
        if (name.startsWith("_")) name = name.substring(1);
        if (instance.getSchemClass().isa("Drug") && instance.getSchemClass().isValidAttribute(ReactomeJavaConstants.drugType)) {
            try {
                GKInstance drugType = (GKInstance) instance.getAttributeValue(ReactomeJavaConstants.drugType);
                return DatabaseObject.class.getPackage().getName() + "." + drugType.getDisplayName();
            } catch (Exception e) {
                return DatabaseObject.class.getPackage().getName() + "." + name;
            }
        } else {
            return DatabaseObject.class.getPackage().getName() + "." + name;
        }
    }

    /**
     * Getting all SimpleNames as neo4j labels, for given class.
     *
     * @param clazz Clazz of object that will result form converting the instance (eg Pathway, Reaction)
     * @return Array of Neo4j SchemaClassCount
     */
    public static Label[] getLabels(Class<?> clazz) {

        if (!labelMap.containsKey(clazz)) {
            Label[] labels = getAllClassNames(clazz).toArray(new Label[]{});
            labelMap.put(clazz, labels);
            return labels;
        } else {
            return labelMap.get(clazz);
        }
    }

    /**
     * Getting all SimpleNames as neo4j labels, for given class.
     *
     * @param clazz Clazz of object that will result form converting the instance (eg Pathway, Reaction)
     * @return Array of Neo4j SchemaClassCount
     */
    private static List<Label> getAllClassNames(Class<?> clazz) {
        return recursiveClassFetcher(clazz, new ArrayList<>());
    }

    private static List<Label> recursiveClassFetcher(Class<?> clazz, List<Label> labels) {
        if (clazz == Object.class) return labels;
        labels.add(Label.label(clazz.getSimpleName()));
        labels.addAll(Arrays.stream(clazz.getAnnotatedInterfaces())
                .filter(type -> type.getType() instanceof Class<?>)
                .map(type -> ((Class<?>) type.getType()))
                .filter(i -> i.getPackage().equals(DatabaseObject.class.getPackage()))
                .map(i -> Label.label(i.getSimpleName()))
                .collect(Collectors.toList()));
        recursiveClassFetcher(clazz.getSuperclass(), labels);
        return labels;
    }


    /**
     * Gets all Fields for specific Class in order to create attribute map.
     * Annotations are used to differentiate attributes:
     *
     * @param clazz Clazz of object that will result form converting the instance (eg Pathway, Reaction)
     * @Relationship is used to indicate a relationship that should be saved to the graph
     * @Transient is used for relationships that should not be persisted by the graph
     * @ReactomeTransient is used for all entries that can not be filled by the GkInstance automatically
     * Not annotated fields will be treated as primitive attributes (String, Long, List<String>...)
     * Twice annotated fields will not be filled by the GkInstance
     */
    @SuppressWarnings("JavaDoc")
    private void setUpFields(Class<?> clazz) {
        if (!relationAttributesMap.containsKey(clazz) && !primitiveAttributesMap.containsKey(clazz)) {
            List<Field> fields = getAllFields(new ArrayList<>(), clazz);
            for (Field field : fields) {
                ReactomeProperty rp = field.getAnnotation(ReactomeProperty.class);
                ReactomeRelationship rr = field.getAnnotation(ReactomeRelationship.class);

                ReactomeAttribute attribute = null;

                if (field.getAnnotation(Relationship.class) != null) {
                    if (field.getAnnotation(ReactomeTransient.class) == null) {
                        boolean addedField = rr != null && rr.addedField();
                        attribute = addFields(relationAttributesMap, clazz, field, addedField);
                    }
                } else if (rp != null) {
                    if (Collection.class.isAssignableFrom(field.getType())) {
                        attribute = addFields(primitiveListAttributesMap, clazz, field, rp.addedField());
                    } else {
                        attribute = addFields(primitiveAttributesMap, clazz, field, rp.addedField());
                    }
                }

                if (rp != null && !rp.originName().isEmpty() && attribute != null) {
                    attributeRenaming.put(attribute, rp.originName());
                } else if (rr != null && !rr.originName().isEmpty() && attribute != null) {
                    attributeRenaming.put(attribute, rr.originName());
                }
                
                // Special case for GO_Term, where accession has been renamed to identifier but cannot be configured.
                if (rp != null && field.getName().equals("identifier") && GO_Term.class.isAssignableFrom(clazz)) {
                    attributeRenaming.put(attribute, ReactomeJavaConstants.accession);
                }
            }
        }
    }

    /**
     * Method used to get all fields for given class, event inherited fields
     *
     * @param fields List of fields for storing fields during recursion
     * @param type   Current class
     * @return inherited and declared fields
     */
    private List<Field> getAllFields(List<Field> fields, Class<?> type) {
        fields.addAll(Arrays.asList(type.getDeclaredFields()));
        if (type.getSuperclass() != null && !type.getSuperclass().equals(Object.class)) {
            fields = getAllFields(fields, type.getSuperclass());
        }
        return fields;
    }

    /**
     * Put Attribute name into map.
     *
     * @param map   attribute map
     * @param clazz Clazz of object that will result form converting the instance (eg Pathway, Reaction)
     */
    private ReactomeAttribute addFields(Map<Class<?>, List<ReactomeAttribute>> map, Class<?> clazz, Field field, boolean addedField) {
        ReactomeAttribute.PropertyType type = !addedField ? getSchemaClassType(clazz, field.getName()) : null;
        Class<?> elementType = Collection.class.isAssignableFrom(field.getType()) ? (Class<?>) ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0] : field.getType();
        ReactomeAttribute attribute = new ReactomeAttribute(field.getName(), type, elementType, clazz);
        if (map.containsKey(clazz)) {
            (map.get(clazz)).add(attribute);
        } else {
            List<ReactomeAttribute> reactomeAttributeList = new ArrayList<>();
            reactomeAttributeList.add(attribute);
            map.put(clazz, reactomeAttributeList);
        }
        return attribute;
    }

    /**
     * Checks if an attributeName is a valid attribute for a specific instance
     *
     * @param instance  GkInstance
     * @param attribute FieldName
     * @return boolean
     */
    private static boolean isValidGkInstanceAttribute(GKInstance instance, String attribute) {
        if (instance.getSchemClass().isValidAttribute(attribute)) {
            return true;
        }
        errorLogger.warn(attribute + " is not a valid attribute for instance " + instance.getSchemClass().getName() + " (" + instance.getDBID() + ")");
        return false;
    }

    /**
     * A simple wrapper of the GkInstance.getAttributeValue Method used for error handling
     *
     * @param instance  GkInstance
     * @param attribute FieldName
     * @return Object
     */
    public static Object getObjectFromGkInstance(GKInstance instance, String attribute) {
        if (isValidGkInstanceAttribute(instance, attribute)) {
            try {
                return instance.getAttributeValue(attribute);
            } catch (Exception e) {
                errorLogger.error("An error occurred when trying to retrieve the '" + attribute + "' from instance with DbId:"
                        + instance.getDBID() + " and Name:" + instance.getDisplayName(), e);
            }
        }
        return null;
    }

    /**
     * A simple wrapper of the GkInstance.getAttributeValueList Method used for error handling
     *
     * @param instance  GkInstance
     * @param attribute FieldName
     * @return Object
     */
    private Collection<GKInstance> getCollectionFromGkInstance(GKInstance instance, String attribute) {
        Collection<GKInstance> rtn = null;
        if (isValidGkInstanceAttribute(instance, attribute)) {
            try {
                rtn = instance.getAttributeValuesList(attribute);
                //In the converter we assume that the empty lists are the result of defensive programming in the
                //GKInstance layer, so we turn those to null to reduce the number of field category check reports
                rtn = (rtn == null || rtn.isEmpty()) ? null : rtn;
            } catch (Exception e) {
                errorLogger.error("An error occurred when trying to retrieve the '" + attribute + "' from instance with DbId:"
                        + instance.getDBID() + " and Name:" + instance.getDisplayName(), e);
            }
        }
        return rtn;
    }

    /**
     * A simple wrapper of the GkInstance.getReferrers Method used for error handling
     *
     * @param instance  GkInstance
     * @param attribute FieldName
     * @return Object
     */
    @SuppressWarnings("SameParameterValue")
    private Collection<GKInstance> getCollectionFromGkInstanceReferrals(GKInstance instance, String attribute) {
        Collection<GKInstance> rtn = null;
        try {
            rtn = instance.getReferers(attribute);
            //In the converter we assume that the empty lists are the result of defensive programming in the
            //GKInstance layer, so we turn those to null to reduce the number of field category check reports
            rtn = (rtn == null || rtn.isEmpty()) ? null : rtn;
        } catch (Exception e) {
            errorLogger.error("An error occurred when trying to retrieve referrals for '" + attribute + "' from instance with DbId:"
                    + instance.getDBID() + " and Name:" + instance.getDisplayName(), e);
        }
        return rtn;
    }

    private Boolean isGKInstanceInCollection(GKInstance instance, Collection<?> collection) {
        try {
            for (Object o : collection) {
                GKInstance gkInstance = (GKInstance) o;
                if (gkInstance.getDBID().equals(instance.getDBID())) return true;
            }
        } catch (Exception e) { /* Nothing here */ }
        return false;
    }

    private Boolean isCuratedEvent(GKInstance instance) {
        try {
            return instance.getAttributeValue("_doRelease") != null;
        } catch (Exception e) {
            errorLogger.error(e.getMessage());
        }
        return false;
    }

    //############################## NEXT BIT IS USED TO CALCULATE THE DATABASE CHECKSUM ###############################

    private Long getDatabaseChecksum() {
        String prefix = "\rCalculating the database checksum: ";
        System.out.print(prefix + "0%");
        long checkSum = 0L;
        @SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
        String queries = "SELECT CONCAT('CHECKSUM TABLE ', table_name, ';') AS statement FROM information_schema.tables WHERE table_schema = ?";
        try {
            Connection dbaConn = dba.getConnection();
            PreparedStatement ps = dbaConn.prepareStatement(queries);
            ps.setString(1, dba.getDBName());

            List<String> checkSumQueries = new ArrayList<>();
            final ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) checkSumQueries.add(resultSet.getString("statement"));
            resultSet.close();

            double total = checkSumQueries.size();
            int i = 0;
            for (String checkSumQuery : checkSumQueries) {
                System.out.print(prefix + Math.round((++i / total) * 100) + "% (please wait...)");
                PreparedStatement css = dbaConn.prepareStatement(checkSumQuery);
                ResultSet cs = css.executeQuery();
                if (cs.next()) checkSum += cs.getLong("Checksum");
                cs.close();
            }
            System.out.println("\rDatabase checksum successfully calculated: " + checkSum);
        } catch (SQLException ex) {
            errorLogger.error(ex.getMessage(), ex);
            System.err.println("\rThere was a problem calculating the database checksum (See error log)");
        }
        return checkSum;
    }

    //##################################### NEXT BIT IS USED FOR CONSISTENCY CHECK #####################################

    private boolean isConsistent(GKInstance instance, Object value, String attribute, ReactomeAttribute.PropertyType type) {
        boolean rtn = false;
        if (type == null) return true;
        if (value == null) {
            if (!type.allowsNull) {
                if (TAXONOMY_ROOT.equals(instance.getDBID()))
                    return true; //Do not report the TAXONOMY_ROOT missing superTaxon
                addConsistencyCheckEntry(instance.getSchemClass().getName(), attribute, type, "null", instance.getDBID(), instance.getDisplayName());
            }
        } else if (value instanceof Collection ? ((Collection<?>) value).isEmpty() : value.toString().isEmpty()) {
            if (!type.allowsEmpty) {
                addConsistencyCheckEntry(instance.getSchemClass().getName(), attribute, type, "empty", instance.getDBID(), instance.getDisplayName());
            }
        } else {
            rtn = true;
        }
        return rtn;
    }

    private ReactomeAttribute.PropertyType getSchemaClassType(Class<?> clazz, String attribute) {
        String className = clazz.getSimpleName();
        //The class ReactionLikeEvent is named ReactionlikeEvent
        className = className.equals(ReactionLikeEvent.class.getSimpleName()) ? "ReactionlikeEvent" : className;
        try {
            switch (dba.fetchSchema().getClassByName(className).getAttribute(attribute).getCategory()) {
                case 1:
                    return ReactomeAttribute.PropertyType.MANDATORY;
                case 2:
                    return ReactomeAttribute.PropertyType.REQUIRED;
                case 3:
                    return ReactomeAttribute.PropertyType.OPTIONAL;
                case 4:
                    return ReactomeAttribute.PropertyType.NOMANUALEDIT;
            }
        } catch (Exception e) { /* Nothing here */ }
        importLogger.info("No category found for attribute '" + attribute + "' in class '" + className + "'. Set to OPTIONAL.");
        return ReactomeAttribute.PropertyType.OPTIONAL;
    }

    //################################## NEXT BIT IS ONLY USED FOR CONSISTENCY REPORT ##################################

    private final Map<String, Map<String, Set<Long>>> consistency = new HashMap<>();
    private int consistencyLoggerEntries = 0;

    private void addConsistencyCheckEntry(String className, String attribute, ReactomeAttribute.PropertyType type, String error, Long dbId, String displayName) {
        consistency.computeIfAbsent(className, k -> new HashMap<>()).computeIfAbsent(attribute, k -> new HashSet<>()).add(dbId);
        if (consistencyLoggerEntries == 0) {
            consistencyCheckReportLogger.error("SchemaClass,Attribute,Category,Error,DbId,DisplayName");
        }
        consistencyCheckReportLogger.error(String.format("%s,%s,%s,%s,%s,\"%s\"", className, attribute, type, error, dbId, displayName));
        consistencyLoggerEntries++;
    }

    private void printConsistencyCheckReport() {
        if (consistencyLoggerEntries == 0) return;
        String aux = consistencyLoggerEntries == 1 ? "entry" : "entries";
        String message = String.format("The consistency check finished reporting %,d %s as follows:", consistencyLoggerEntries, aux);
        List<String> lines = new ArrayList<>();
        consistency.forEach((className, attributes) ->
                attributes.forEach((attribute, instances) -> {
                    String entries = instances.size() == 1 ? "entry" : "entries";
                    lines.add(String.format("\t%,10d %s for (%s, %s)", instances.size(), entries, className, attribute));
                })
        );
        lines.sort(Collections.reverseOrder());

        System.out.println();
        System.out.println(message);
        lines.forEach(System.out::println);

        //Also keep in the log file (just in case)
        consistencyCheckSummaryLogger.info(message);
        lines.forEach(consistencyCheckSummaryLogger::info);
    }
}