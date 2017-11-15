package org.reactome.server.graph.batchimport;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ClassUtils;
import org.apache.commons.lang.IllegalClassException;
import org.gk.model.GKInstance;
import org.gk.model.ReactomeJavaConstants;
import org.gk.pathwaylayout.DiagramGeneratorFromDB;
import org.gk.persistence.MySQLAdaptor;
import org.gk.schema.InvalidClassException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.ogm.annotation.Relationship;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.reactome.server.graph.domain.annotations.ReactomeProperty;
import org.reactome.server.graph.domain.annotations.ReactomeRelationship;
import org.reactome.server.graph.domain.annotations.ReactomeTransient;
import org.reactome.server.graph.domain.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ReactomeBatchImporter {

    private static final Logger errorLogger = LoggerFactory.getLogger("import_error");
    private static final Logger importLogger = LoggerFactory.getLogger("import");
    private static final Logger consistencyLogger = LoggerFactory.getLogger("consistency_report");

    private static MySQLAdaptor dba;
    private static BatchInserter batchInserter;
    private static String DATA_DIR;

    private static final String DBID = "dbId";
    private static final String STID = "stId";
    private static final String OLD_STID = "oldStId";
    private static final Long TAXONOMY_ROOT = 164487L;
    private static final String TAXONOMY_ID = "taxId";
    private static final String ACCESSION = "identifier";
    private static final String NAME = "displayName";

    private static final String STOICHIOMETRY = "stoichiometry";
    private static final String ORDER = "order";

    private static final Map<Class, List<ReactomeAttribute>> primitiveAttributesMap = new HashMap<>();
    private static final Map<Class, List<ReactomeAttribute>> primitiveListAttributesMap = new HashMap<>();
    private static final Map<Class, List<ReactomeAttribute>> relationAttributesMap = new HashMap<>();

    private static final Map<Class, Label[]> labelMap = new HashMap<>();
    private static final Map<Long, Long> dbIds = new HashMap<>();
    private static final Map<Long, Long> reverseReactions = new HashMap<>();
    private static final Map<Long, Long> equivalentTo = new HashMap<>();

    private static final Set<Long> topLevelPathways = new HashSet<>();

    private static final int width = 70;
    private static int total;

    private static final DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public ReactomeBatchImporter(String host, Integer port, String name, String user, String password, String neo4j) {
        try {
            DATA_DIR = neo4j;
            dba = new MySQLAdaptor(host, name, user, password, port);
            total = (int) dba.getClassInstanceCount(ReactomeJavaConstants.DatabaseObject);
            total = total - (int) dba.getClassInstanceCount(ReactomeJavaConstants.StableIdentifier);
            total = total - (int) dba.getClassInstanceCount(ReactomeJavaConstants.PathwayDiagramItem);
            total = total - (int) dba.getClassInstanceCount(ReactomeJavaConstants.ReactionCoordinates);
            importLogger.info("Established connection to Reactome database");
        } catch (SQLException | InvalidClassException e) {
            importLogger.error("An error occurred while connection to the Reactome database", e);
        }
    }

    public void importAll(boolean barComplete) throws IOException {
        Long start = System.currentTimeMillis();
        prepareDatabase();
        try {
            List<GKInstance> tlps = getTopLevelPathways();
            importLogger.info("Started importing " + tlps.size() + " top level pathways");
            System.out.println("Started importing " + tlps.size() + " top level pathways...\n");

            addDbInfo();

            for (GKInstance instance : tlps) {
                long instanceStart = System.currentTimeMillis();
                if (!dbIds.containsKey(instance.getDBID())) {
                    importGkInstance(instance);
                }
                long elapsedTime = System.currentTimeMillis() - instanceStart;
                int ms = (int) elapsedTime % 1000;
                int sec = (int) (elapsedTime / 1000) % 60;
                int min = (int) ((elapsedTime / (1000 * 60)) % 60);
                importLogger.info(instance.getDisplayName() + " was processed within: " + min + " min " + sec + " sec " + ms + " ms");
            }
            if (barComplete) updateProgressBar(total); //This is just forcing a 100% in the progress bar
        } catch (Exception e) {
            e.printStackTrace();
        }
        printConsistencyCheckReport();
        importLogger.info("Storing the graph");
        System.out.println("\nPlease wait while storing the graph...");
        batchInserter.shutdown();
        importLogger.info("All top level pathways have been imported to Neo4j");
        Long time = System.currentTimeMillis() - start;
        System.out.println("\rAll top level pathways have been imported to Neo4j (" + getTimeFormatted(time) + ")");

    }

    private void addDbInfo() throws Exception {
        Map<String, Object> properties = new HashMap<>();
        properties.put("name", dba.getDBName());
        properties.put("version", dba.getReleaseNumber());
        batchInserter.createNode(properties, Label.label("DBInfo"));
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

    /**
     * Imports one single GkInstance into neo4j.
     * When iterating through the relationAttributes it is possible to go deeper
     * into the GkInstance hierarchy (eg hasEvents)
     *
     * @param instance GkInstance
     * @return Neo4j native id (generated by the BatchInserter)
     */
    @SuppressWarnings("unchecked")
    private Long importGkInstance(GKInstance instance) throws ClassNotFoundException {

        if (dbIds.size() != 0 && dbIds.size() % 100 == 0) updateProgressBar(dbIds.size());

        String clazzName = DatabaseObject.class.getPackage().getName() + "." + instance.getSchemClass().getName();
        Class clazz = Class.forName(clazzName);
        setUpFields(clazz); //Sets up the attribute map per class populating relationAttributesMap and primitiveListAttributesMap
        Long id = saveDatabaseObject(instance, clazz);
        dbIds.put(instance.getDBID(), id); //caching the "saved" object mapped to the corresponding Neo4j node id

        if (relationAttributesMap.containsKey(clazz)) {
            for (ReactomeAttribute reactomeAttribute : relationAttributesMap.get(clazz)) {
                String attribute = reactomeAttribute.getAttribute();
                ReactomeAttribute.PropertyType type = reactomeAttribute.getType();
                switch (attribute) {
                    case "regulatedBy":
                        /*
                         * Only one type of regulation is needed here, In the native data import only regulatedBy exists
                         * since the type of regulation is later determined by the Object Type we can only save one
                         * otherwise relationships will be duplicated
                         * if event will break otherwise (physical entity will fall to default
                         */
                        //saveRelations4hips might enter in recursion so changes in "positivelyRegulatedBy" have to be carefully thought
                        saveRelationships(id, getCollectionFromGkInstanceReferrals(instance, ReactomeJavaConstants.regulatedEntity), attribute);
                        break;
                    case "orthologousEvent":
                        if (isCuratedEvent(instance)) {
                            Collection<GKInstance> orthologousAll = getCollectionFromGkInstance(instance, attribute);
                            if (orthologousAll != null && !orthologousAll.isEmpty()) {
                                Collection alreadyPointing = getCollectionFromGkInstanceReferrals(instance, ReactomeJavaConstants.inferredFrom);
                                //orthologousEvents collection will only contain those that are not pointing to instance as inferredFrom to avoid inferredTo duplicates
                                Collection orthologousEvents = new ArrayList();
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
                        if (isValidGkInstanceAttribute(instance, attribute)) {
                            Collection inferredFrom = getCollectionFromGkInstance(instance, attribute);
                            saveRelationships(id, inferredFrom, "inferredToReverse");
                        }
                        break;
                    case "inferredTo": //Only for PhysicalEntity
                        if (isValidGkInstanceAttribute(instance, attribute)) {
                            Collection inferredTo = getCollectionFromGkInstance(instance, attribute);
                            if (inferredTo == null || inferredTo.isEmpty()) {
                                inferredTo = getCollectionFromGkInstanceReferrals(instance, ReactomeJavaConstants.inferredFrom);
                            }
                            saveRelationships(id, inferredTo, attribute);
                        }
                        break;
                    default:
                        if (isValidGkInstanceAttribute(instance, attribute)) {
                            Collection relationships = getCollectionFromGkInstance(instance, attribute);
                            if (isConsistent(instance, relationships, attribute, type)) {
                                //saveRelationships might enter in recursion so changes in "orthologousEvent" have to be carefully thought
                                saveRelationships(id, relationships, attribute);
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
    private Long saveDatabaseObject(GKInstance instance, Class clazz) throws IllegalArgumentException {

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
                String attribute = reactomeAttribute.getAttribute();
                ReactomeAttribute.PropertyType type = reactomeAttribute.getType();
                switch (attribute) {
                    case STID:
                        GKInstance stableIdentifier = (GKInstance) getObjectFromGkInstance(instance, ReactomeJavaConstants.stableIdentifier);
                        if (stableIdentifier == null) continue;
                        String stId = (String) getObjectFromGkInstance(stableIdentifier, ReactomeJavaConstants.identifier);
                        if (stId == null) continue;
                        properties.put(attribute, stId);
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
                    case "orcidId":
                        GKInstance orcid = (GKInstance) getObjectFromGkInstance(instance, ReactomeJavaConstants.crossReference);
                        if (orcid == null) continue;
                        String orcidId = (String) getObjectFromGkInstance(orcid, ReactomeJavaConstants.identifier);
                        if (orcidId == null) continue;
                        properties.put(attribute, orcidId);
                        break;
                    case TAXONOMY_ID:
                        GKInstance taxon = (GKInstance) getObjectFromGkInstance(instance, ReactomeJavaConstants.crossReference);
                        String taxId = taxon != null ? (String) getObjectFromGkInstance(taxon, ReactomeJavaConstants.identifier) : null;
                        if (taxId != null && !taxId.isEmpty()) {
                            properties.put(attribute, taxId);
                        } else if (!instance.getDBID().equals(TAXONOMY_ROOT)) {
                            errorLogger.warn("'" + TAXONOMY_ID + "' cannot be set for " + instance.getDBID() + ": " + instance.getDisplayName());
                        }
                        break;
                    case "hasDiagram":
                        if (instance.getDbAdaptor() instanceof MySQLAdaptor) {
                            try {
                                DiagramGeneratorFromDB diagramHelper = new DiagramGeneratorFromDB();
                                diagramHelper.setMySQLAdaptor((MySQLAdaptor) instance.getDbAdaptor());
                                GKInstance diagram = diagramHelper.getPathwayDiagram(instance);
                                properties.put(attribute, diagram != null);
                            } catch (Exception e) {
                                errorLogger.error("An exception occurred while trying to retrieve a diagram from entry with dbId: " +
                                        instance.getDBID() + "and name: " + instance.getDisplayName());
                            }
                        }
                        break;
                    case "isInDisease":
                        GKInstance disease = (GKInstance) getObjectFromGkInstance(instance, ReactomeJavaConstants.disease);
                        properties.put(attribute, disease != null);
                        break;
                    case "isInferred":
                        GKInstance isInferredFrom = (GKInstance) getObjectFromGkInstance(instance, ReactomeJavaConstants.inferredFrom);
                        properties.put(attribute, isInferredFrom != null);
                        break;
                    case "referenceType":
                        GKInstance referenceEntity = (GKInstance) getObjectFromGkInstance(instance, ReactomeJavaConstants.referenceEntity);
                        if (referenceEntity == null) continue;
                        properties.put(attribute, referenceEntity.getSchemClass().getName());
                        break;
                    case "speciesName":
                        if (instance.getSchemClass().isa(ReactomeJavaConstants.OtherEntity)) continue;
                        if (instance.getSchemClass().isa(ReactomeJavaConstants.ChemicalDrug)) continue;
                        List speciesList = (List) getCollectionFromGkInstance(instance, ReactomeJavaConstants.species);
                        if (speciesList == null || speciesList.isEmpty()) continue;
                        GKInstance species = (GKInstance) speciesList.get(0);
                        properties.put(attribute, species.getDisplayName());
                        break;
                    case "url":
                        if (!instance.getSchemClass().isa(ReactomeJavaConstants.ReferenceDatabase) && !instance.getSchemClass().isa(ReactomeJavaConstants.Figure)) {
                            GKInstance referenceDatabase = (GKInstance) getObjectFromGkInstance(instance, ReactomeJavaConstants.referenceDatabase);
                            if (referenceDatabase == null) continue;
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
                            properties.put("databaseName", referenceDatabase.getDisplayName());
                            properties.put(attribute, url.replace("###ID###", identifier));
                            break;
                        }
                    default: //Here we are in a none graph-added field. The field content has to be treated based on the schema definition
                        if (isValidGkInstanceAttribute(instance, attribute)) {
                            Object value = getObjectFromGkInstance(instance, attribute);
                            if (isConsistent(instance, value, attribute, type)) {
                                properties.put(attribute, value);
                            }
                        }
                        break;
                }
            }
        }

        // Last thing is iterating across all the list of objects previously mapped in primitiveListAttributesMap
        if (primitiveListAttributesMap.containsKey(clazz)) {
            for (ReactomeAttribute reactomeAttribute : primitiveListAttributesMap.get(clazz)) {
                String attribute = reactomeAttribute.getAttribute();
                ReactomeAttribute.PropertyType type = reactomeAttribute.getType();
                if (isValidGkInstanceAttribute(instance, attribute)) {
                    Collection values = getCollectionFromGkInstance(instance, attribute);
                    if (isConsistent(instance, values, attribute, type)) {
                        properties.put(attribute, values.toArray(new String[values.size()]));
                    }
                }
            }
        }

        // The node is now ready to be inserted in the graph database
        try {
            return batchInserter.createNode(properties, labels);
        } catch (IllegalArgumentException e) {
            throw new IllegalClassException("A problem occurred when trying to save entry to the Graph :" + instance.getDisplayName() + ":" + instance.getDBID());
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
    private void saveRelationships(Long oldId, Collection objects, String relationName) throws ClassNotFoundException {
        if (objects == null || objects.isEmpty()) return;

        if (relationName.equals("modified")) {
            try {
                GKInstance latestModified = (GKInstance) objects.iterator().next();
                Date latestDate = formatter.parse((String) getObjectFromGkInstance(latestModified, ReactomeJavaConstants.dateTime));
                for (Object object : objects) {
                    GKInstance gkInstance = (GKInstance) object;
                    Date date = formatter.parse((String) getObjectFromGkInstance(gkInstance, ReactomeJavaConstants.dateTime));
                    if (latestDate.before(date)) {
                        latestDate = date;
                        latestModified = gkInstance;
                    }
                }
                objects.clear();
                //noinspection unchecked
                objects.add(latestModified);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        Map<Long, GkInstancePropertiesHelper> propertiesMap = new HashMap<>();
        //noinspection unchecked
        objects.stream().filter(object -> object instanceof GKInstance).forEach(object -> {
            GKInstance instance = (GKInstance) object;
            if (propertiesMap.containsKey(instance.getDBID())) {
                propertiesMap.get(instance.getDBID()).increment();
            } else {
                int order = propertiesMap.keySet().size();
                propertiesMap.put(instance.getDBID(), new GkInstancePropertiesHelper(instance, order));
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
        batchInserter = BatchInserters.inserter(file);
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
        createDeferredSchemaIndex(ReferenceEntity.class, ACCESSION);
    }

    /**
     * Simple wrapper for creating a isUnique constraint
     *
     * @param clazz specific Class
     * @param name  fieldName
     */
    private static void createSchemaConstraint(Class clazz, String name) {
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
    private static void createDeferredSchemaIndex(Class clazz, String name) {
        try {
            batchInserter.createDeferredSchemaIndex(Label.label(clazz.getSimpleName())).on(name).create();
        } catch (Throwable e) {
            //ConstraintViolationException and PreexistingIndexEntryConflictException are both catch here
            importLogger.warn("Could not create Index on " + clazz.getSimpleName() + " for " + name);
        }
    }

    /**
     * Simple method that prints a progress bar to command line
     *
     * @param done Number of entries added to the graph
     */
    private void updateProgressBar(int done) {
        String format = "\r%3d%% %s %c";
        char[] rotators = {'|', '/', '—', '\\'};
        double percent = (double) done / total;
        StringBuilder progress = new StringBuilder(width);
        progress.append('|');
        int i = 0;
        for (; i < (int) (percent * width); i++) progress.append("=");
        for (; i < width; i++) progress.append(" ");
        progress.append('|');
        System.out.printf(format, (int) (percent * 100), progress, rotators[((done - 1) % (rotators.length * 100)) / 100]);
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
     * Getting all SimpleNames as neo4j labels, for given class.
     *
     * @param clazz Clazz of object that will result form converting the instance (eg Pathway, Reaction)
     * @return Array of Neo4j SchemaClassCount
     */
    private Label[] getLabels(Class clazz) {

        if (!labelMap.containsKey(clazz)) {
            Label[] labels = getAllClassNames(clazz);
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
    private Label[] getAllClassNames(Class clazz) {
        List<?> superClasses = ClassUtils.getAllSuperclasses(clazz);
        List<Label> labels = new ArrayList<>();
        labels.add(Label.label(clazz.getSimpleName()));
        for (Object object : superClasses) {
            Class superClass = (Class) object;
            if (!superClass.equals(Object.class)) {
                labels.add(Label.label(superClass.getSimpleName()));
            }
        }
        return labels.toArray(new Label[labels.size()]);
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
    private void setUpFields(Class clazz) {
        if (!relationAttributesMap.containsKey(clazz) && !primitiveAttributesMap.containsKey(clazz)) {
            List<Field> fields = getAllFields(new ArrayList<>(), clazz);
            for (Field field : fields) {
                String fieldName = field.getName();
                if (field.getAnnotation(Relationship.class) != null) {
                    if (field.getAnnotation(ReactomeTransient.class) == null) {
                        boolean addedField = field.getAnnotation(ReactomeRelationship.class) != null;
                        addFields(relationAttributesMap, clazz, fieldName, addedField);
                    }
                } else if (field.getAnnotation(ReactomeProperty.class) != null) {
                    ReactomeProperty rp = field.getAnnotation(ReactomeProperty.class);
                    if (Collection.class.isAssignableFrom(field.getType())) {
                        addFields(primitiveListAttributesMap, clazz, fieldName, rp.addedField());
                    } else {
                        addFields(primitiveAttributesMap, clazz, fieldName, rp.addedField());
                    }
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
    private void addFields(Map<Class, List<ReactomeAttribute>> map, Class clazz, String field, boolean addedField) {
        ReactomeAttribute.PropertyType type = !addedField ? getSchemaClassType(clazz, field) : null;
        if (map.containsKey(clazz)) {
            (map.get(clazz)).add(new ReactomeAttribute(field, type));
        } else {
            List<ReactomeAttribute> reactomeAttributeList = new ArrayList<>();
            reactomeAttributeList.add(new ReactomeAttribute(field, type));
            map.put(clazz, reactomeAttributeList);
        }
    }

    /**
     * Checks if an attributeName is a valid attribute for a specific instance
     *
     * @param instance  GkInstance
     * @param attribute FieldName
     * @return boolean
     */
    private boolean isValidGkInstanceAttribute(GKInstance instance, String attribute) {
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
    private Object getObjectFromGkInstance(GKInstance instance, String attribute) {
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
    private Collection getCollectionFromGkInstance(GKInstance instance, String attribute) {
        Collection rtn = null;
        if (isValidGkInstanceAttribute(instance, attribute)) {
            try {
                rtn = instance.getAttributeValuesList(attribute);
                //In the converter we assume that the empty lists are the result of defensive programming in the
                //GKInstance layer, so we turn those to null to reduce the number of field category check reports
                rtn = ( rtn == null || rtn.isEmpty() ) ? null : rtn;
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
    private Collection getCollectionFromGkInstanceReferrals(GKInstance instance, String attribute) {
        Collection rtn = null;
        try {
            rtn = instance.getReferers(attribute);
            //In the converter we assume that the empty lists are the result of defensive programming in the
            //GKInstance layer, so we turn those to null to reduce the number of field category check reports
            rtn = ( rtn == null || rtn.isEmpty() ) ? null : rtn;
        } catch (Exception e) {
            errorLogger.error("An error occurred when trying to retrieve referrals for '" + attribute + "' from instance with DbId:"
                    + instance.getDBID() + " and Name:" + instance.getDisplayName(), e);
        }
        return rtn;
    }

    private Boolean isGKInstanceInCollection(GKInstance instance, Collection collection) {
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

    //##################################### NEXT BIT IS USED FOR CONSISTENCY CHECK #####################################

    private boolean isConsistent(GKInstance instance, Object value, String attribute, ReactomeAttribute.PropertyType type){
        boolean rtn = false;
        if (value == null) {
            if (!type.allowsNull) {
                addConsistencyCheckEntry(instance.getSchemClass().getName(),attribute, type ,"null" ,instance.getDBID(), instance.getDisplayName());
            }
        } else if (value instanceof Collection ? ((Collection) value).isEmpty() : value.toString().isEmpty()) {
            if (!type.allowsEmpty) {
                addConsistencyCheckEntry(instance.getSchemClass().getName(),attribute, type ,"empty" ,instance.getDBID(), instance.getDisplayName());
            }
        } else {
            rtn = true;
        }
        return rtn;
    }

    private ReactomeAttribute.PropertyType getSchemaClassType(Class clazz, String attribute) {
        String className = clazz.getSimpleName();
        //The class ReactionLikeEvent is named ReactionlikeEvent
        className = className.equals(ReactionLikeEvent.class.getSimpleName()) ? "ReactionlikeEvent" : className;
        try {
            switch (dba.fetchSchema().getClassByName(className).getAttribute(attribute).getCategory()) {
                case 1: return ReactomeAttribute.PropertyType.MANDATORY;
                case 2: return ReactomeAttribute.PropertyType.REQUIRED;
                case 3: return ReactomeAttribute.PropertyType.OPTIONAL;
                case 4: return ReactomeAttribute.PropertyType.NOMANUALEDIT;
            }
        } catch (Exception e) { /* Nothing here */ }
        importLogger.info("No category found for attribute '" + attribute + "' in class '" + className + "'. Set to OPTIONAL.");
        return ReactomeAttribute.PropertyType.OPTIONAL;
    }

    //################################## NEXT BIT IS ONLY USED FOR CONSISTENCY REPORT ##################################

    private Map<String, Map<String, Set<Long>>> consistency = new HashMap<>();
    private int consistencyLoggerEntries = 0;

    private void addConsistencyCheckEntry(String className, String attribute, ReactomeAttribute.PropertyType type, String error, Long dbId, String displayName) {
        consistency.computeIfAbsent(className, k -> new HashMap<>()).computeIfAbsent(attribute, k-> new HashSet<>()).add(dbId);
        if (consistencyLoggerEntries == 0) {
            consistencyLogger.error("SchemaClass,Attribute,Category,Error,DbId,DisplayName");
        }
        consistencyLogger.error(String.format("%s,%s,%s,%s,%s,\"%s\"", className, attribute, type, error, dbId, displayName));
        consistencyLoggerEntries++;
    }

    private void printConsistencyCheckReport(){
        if(consistencyLoggerEntries == 0) return;

        String message = String.format("\n\nThe consistency check finished reporting %,d as follows:", consistencyLoggerEntries);
        List<String> lines = new ArrayList<>();
        consistency.forEach((className, attributes) ->
                attributes.forEach((attribute, instances) ->
                    lines.add(String.format("\t%,10d entries for (%s, %s)", instances.size(), className, attribute))
                )
        );
        lines.sort(Collections.reverseOrder());

        System.out.println(message);
        lines.forEach(System.out::println);

        //Also keep in the log file (just in case)
        importLogger.info(message);
        lines.forEach(importLogger::info);
    }

    private static String getTimeFormatted(Long millis) {
        return String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(millis),
                TimeUnit.MILLISECONDS.toMinutes(millis) % TimeUnit.HOURS.toMinutes(1),
                TimeUnit.MILLISECONDS.toSeconds(millis) % TimeUnit.MINUTES.toSeconds(1));
    }
}