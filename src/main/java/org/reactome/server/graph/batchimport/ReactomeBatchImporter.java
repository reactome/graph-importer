package org.reactome.server.graph.batchimport;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ClassUtils;
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
import org.reactome.server.graph.curator.domain.annotations.ReactomeProperty;
import org.reactome.server.graph.curator.domain.annotations.ReactomeRelationship;
import org.reactome.server.graph.curator.domain.annotations.ReactomeTransient;
import org.reactome.server.graph.curator.domain.model.*;
import org.reactome.server.graph.utils.GKInstanceHelper;
import org.reactome.server.graph.utils.ProgressBarUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.neo4j.core.schema.Relationship;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static org.reactome.server.graph.utils.FormatUtils.getTimeFormatted;

public class ReactomeBatchImporter {

    private static final Logger importLogger = LoggerFactory.getLogger("import");
    private static final Logger errorLogger = LoggerFactory.getLogger("import_error");
    private static final Logger consistencyCheckSummaryLogger = LoggerFactory.getLogger("consistency_check_summary");
    private static final Logger consistencyCheckReportLogger = LoggerFactory.getLogger("consistency_check_report");

    private static MySQLAdaptor dba;
    private static BatchInserter batchInserter;
    private static String DATA_DIR;
    private String neo4jVersion;

    private static final String DBID = "dbId";
    private static final String STABLE_IDENTIFIER = "stableIdentifier";
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

    public static Long maxDbId;
    private static final Map<Long, Long> dbIds = new HashMap<>();
    private static final Set<Long> discarded = new HashSet<>();
    private static final Map<Long, Long> reverseReactions = new HashMap<>();
    private static final Map<Long, Long> equivalentTo = new HashMap<>();
    private static final Map<Class<?>, Label[]> labelMap = new HashMap<>();
    private static final Map<Integer, Long> taxIdDbId = new HashMap<>();

    private static final Set<Long> topLevelPathways = new HashSet<>();

    private static final List<String> curationOnlyClassNames = Arrays.asList(
            "_DeletedInstance",
            ReactomeJavaConstants._Deleted,
            ReactomeJavaConstants._Release,
            ReactomeJavaConstants._UpdateTracker,
            ReactomeJavaConstants.FrontPage,
            ReactomeJavaConstants.PathwayDiagram
    );

    private static int total;

    private static final DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private Set<String> trivialMolecules;
    private final GKInstanceHelper gkInstanceHelper;

    public ReactomeBatchImporter(String host, Integer port, String name, String user, String password, String neo4j,
                                 boolean includeInteractors, String interactorsFile, String neo4jVersion) {
        try {
            DATA_DIR = neo4j;
            this.neo4jVersion = neo4jVersion;
            dba = new MySQLAdaptor(host, name, user, password, port);
            maxDbId = dba.fetchMaxDbId();

            total = (int) dba.getClassInstanceCount(ReactomeJavaConstants.DatabaseObject);
            total = total - (int) dba.getClassInstanceCount(ReactomeJavaConstants.StableIdentifier);
            total = total - (int) dba.getClassInstanceCount(ReactomeJavaConstants.PathwayDiagramItem);
            total = total - (int) dba.getClassInstanceCount(ReactomeJavaConstants.ReactionCoordinates);
            total = total - (int) dba.getClassInstanceCount(ReactomeJavaConstants._Release);
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

    public void importAll(boolean barComplete) throws IOException {
        final long start = System.currentTimeMillis();
        prepareDatabase();

        try {
            addDbInfo();
        } catch (Exception e) {
            String msg = "Database Information node cannot be created. The expected information is not present in the relational database.";
            System.out.println(msg);
            importLogger.warn(msg);
        }

        try {
            // Import Curation model-only classes
            for (String className : curationOnlyClassNames) {
                Iterator iter = dba.fetchInstancesByClass(className).iterator();
                while (iter.hasNext()) {
                    GKInstance instance = (GKInstance) iter.next();
                    if (!dbIds.containsKey(instance.getDBID())) {
                        importGkInstance(instance);
                    }
                }
            }
            // Import pathways
            List<GKInstance> tlps = getTopLevelPathways();
            importLogger.info("Started importing " + tlps.size() + " top level pathways");
            System.out.println("Started importing " + tlps.size() + " top level pathways...\n");
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
            if (barComplete) ProgressBarUtils.completeProgressBar(total); //This is just forcing a 100% in the progress bar
        } catch (Exception e) {
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

    private void addDbInfo() throws Exception {
        Map<String, Object> properties = new HashMap<>();
        properties.put("name", dba.getDBName());
        properties.put("version", dba.getReleaseNumber());
        properties.put("checksum", getDatabaseChecksum());
        properties.put("neo4j", getNeo4jVersion());
        batchInserter.createNode(properties, Label.label("DBInfo"));
    }

    /**
     * Default 3.5.x
     * @return neo4j installed in the system that is running on the system.
     */
    private String getNeo4jVersion() {
        try {
            ProcessBuilder pb = new ProcessBuilder("/usr/bin/neo4j", "version");
            neo4jVersion = IOUtils.toString(pb.start().getInputStream(), StandardCharsets.UTF_8).trim();
        } catch (Exception e) {
            System.err.println("[NEO4J VERSION] Failed to determine OS neo4j version. Default value from PROGRAM ARGUMENTS will be used [" + neo4jVersion + "]. Make sure you are setting --neo4jVersion appropriately");
            importLogger.error("[NEO4J VERSION] Failed to determine OS neo4j version. Default value from PROGRAM ARGUMENTS will be used [" + neo4jVersion + "]. Make sure you are setting --neo4jVersion appropriately");
        }
        return neo4jVersion;
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
    private Long importGkInstance(GKInstance instance) throws ClassNotFoundException {
        ProgressBarUtils.updateProgressBar(dbIds.size() + discarded.size(), total);

        String clazzName = getClassName(instance);
        Class<?> clazz = Class.forName(clazzName);
        setUpFields(clazz); //Sets up the attribute map per class populating relationAttributesMap and primitiveListAttributesMap
        Long id = saveDatabaseObject(instance, clazz);
        dbIds.put(instance.getDBID(), id); //caching the "saved" object mapped to the corresponding Neo4j node id

        if (relationAttributesMap.containsKey(clazz)) {
            for (ReactomeAttribute reactomeAttribute : relationAttributesMap.get(clazz)) {
                String attribute = reactomeAttribute.getAttribute();
                ReactomeAttribute.PropertyType type = reactomeAttribute.getType();
                switch (attribute) {
                    case "orthologousEvent":
                        if (isCuratedEvent(instance)) {
                            Collection<GKInstance> orthologousAll = getCollectionFromGkInstance(instance, attribute);
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
                        if (isValidGkInstanceAttribute(instance, attribute)) {
                            Collection<GKInstance> inferredFrom = getCollectionFromGkInstance(instance, attribute);
                            saveRelationships(id, inferredFrom, "inferredToReverse");
                        }
                        break;
                    case "inferredTo": //Only for PhysicalEntity
                        if (isValidGkInstanceAttribute(instance, attribute)) {
                            Collection<GKInstance> inferredTo = getCollectionFromGkInstance(instance, attribute);
                            if (inferredTo == null || inferredTo.isEmpty()) {
                                inferredTo = getCollectionFromGkInstanceReferrals(instance, ReactomeJavaConstants.inferredFrom);
                            }
                            saveRelationships(id, inferredTo, attribute);
                        }
                        break;
                    default:
                        if (isValidGkInstanceAttribute(instance, attribute)) {
                            Collection<GKInstance> relationships = getCollectionFromGkInstance(instance, attribute);
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
                String attribute = reactomeAttribute.getAttribute();
                ReactomeAttribute.PropertyType type = reactomeAttribute.getType();
                switch (attribute) {
                    case "className": // _DeletedInstance
                        if (instance.getSchemClass().getName().equals("_DeletedInstance")) {
                            String className = (String) getObjectFromGkInstance(instance, "class");
                            properties.put(attribute, className);
                        }
                        break;
                    case "release": // _UpdateTracker
                        if (instance.getSchemClass().getName().equals("_UpdateTracker")) {
                            _Release className = (_Release) getObjectFromGkInstance(instance, "_" + attribute);
                            properties.put(attribute, className);
                        }
                        break;
                    case "doRelease": // Event
                        Boolean doRelease = (Boolean) getObjectFromGkInstance(instance, "_" + attribute);
                        properties.put(attribute, doRelease != null && doRelease);
                        break;
                    case "chainChangeLog": // ReferenceGeneProduct
                        Object chainChangeLog = getObjectFromGkInstance(instance, "_" + attribute);
                        if (chainChangeLog != null) {
                            properties.put(attribute, chainChangeLog.toString());
                        }
                        break;
                    case "hasEHLD":
                        Boolean hasEHLD = (Boolean) getObjectFromGkInstance(instance, ReactomeJavaConstants.hasEHLD);
                        properties.put(attribute, hasEHLD != null && hasEHLD);
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
                            properties.put(attribute, url.replace("###ID###", identifier));
                        } else {
                            defaultAction(instance, attribute, properties, type);   //Can be added or existing
                        }
                        break;
                    default: //Here we are in a none graph-added field. The field content has to be treated based on the schema definition
                        defaultAction(instance, attribute, properties, type);
                }
            }
        }

        // Last thing is iterating across all the list of objects previously mapped in primitiveListAttributesMap
        if (primitiveListAttributesMap.containsKey(clazz)) {
            for (ReactomeAttribute reactomeAttribute : primitiveListAttributesMap.get(clazz)) {
                String attribute = reactomeAttribute.getAttribute();

                ReactomeAttribute.PropertyType type = reactomeAttribute.getType();
                if (isValidGkInstanceAttribute(instance, attribute)) {
                    Collection<?> values = getCollectionFromGkInstance(instance, attribute);
                    if (isConsistent(instance, values, attribute, type)) {
                        //noinspection SuspiciousToArrayCall,ToArrayCallWithZeroLengthArrayArgument
                        if (!values.isEmpty() && values.iterator().next().getClass().getSimpleName().equals("Integer")) {
                            // e.g. _Deleted.deletedInstanceDB_ID
                            properties.put(attribute, values.stream().map(x ->
                                    Integer.toString((int) x)).collect(Collectors.toList()).toArray(new String[values.size()]));
                        } else {
                            properties.put(attribute, values.toArray(new String[values.size()]));
                        }
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
    private void defaultAction(GKInstance instance, String attribute, Map<String, Object> properties, ReactomeAttribute.PropertyType type){
        if (isValidGkInstanceAttribute(instance, attribute)) {
            Object value = getObjectFromGkInstance(instance, attribute);
            if (isConsistent(instance, value, attribute, type)) {
                properties.put(attribute, value);
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
                    if (!dbIds.containsKey(gkInstance.getDBID())) discarded.add(gkInstance.getDBID());
                    Date date = formatter.parse((String) getObjectFromGkInstance(gkInstance, ReactomeJavaConstants.dateTime));
                    if (latestDate.before(date)) {
                        latestDate = date;
                        latestModified = gkInstance;
                    }
                }
                objects.clear();
                objects.add(latestModified);
                discarded.remove(latestModified.getDBID());
            } catch (Exception e) {
                importLogger.error("Problem while filtering tbe 'modified' relationship for " + oldId, e);
            }
        }

        Map<Long, GkInstancePropertiesHelper> propertiesMap = new HashMap<>();
        objects.stream().filter(Objects::nonNull).forEach(object -> {
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

    @SuppressWarnings("Duplicates")
    public static void saveRelationship(Long toId, Long fromId, RelationshipType relationshipType, Map<String, Object> properties) {
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

        batchInserter = BatchInserters.inserter(DatabaseLayout.ofFlat(file.toPath()));
        createConstraints();
    }

    /**
     * Creating uniqueness constraints and indexes for the new DB.
     * WARNING: Constraints can not be enforced while importing, only after batchInserter.shutdown()
     */
    private void createConstraints() {

        createSchemaConstraint(DatabaseObject.class, DBID);
        createSchemaConstraint(DatabaseObject.class, STABLE_IDENTIFIER);

        createSchemaConstraint(Event.class, DBID);
        createSchemaConstraint(Event.class, STABLE_IDENTIFIER);

        createSchemaConstraint(Pathway.class, DBID);
        createSchemaConstraint(Pathway.class, STABLE_IDENTIFIER);

        createSchemaConstraint(ReactionLikeEvent.class, DBID);
        createSchemaConstraint(ReactionLikeEvent.class, STABLE_IDENTIFIER);

        createSchemaConstraint(Reaction.class, DBID);
        createSchemaConstraint(Reaction.class, STABLE_IDENTIFIER);

        createSchemaConstraint(PhysicalEntity.class, DBID);
        createSchemaConstraint(PhysicalEntity.class, STABLE_IDENTIFIER);

        createSchemaConstraint(Complex.class, DBID);
        createSchemaConstraint(Complex.class, STABLE_IDENTIFIER);

        createSchemaConstraint(EntitySet.class, DBID);
        createSchemaConstraint(EntitySet.class, STABLE_IDENTIFIER);

        createSchemaConstraint(GenomeEncodedEntity.class, DBID);
        createSchemaConstraint(GenomeEncodedEntity.class, STABLE_IDENTIFIER);

        createSchemaConstraint(ReferenceEntity.class, DBID);
        createSchemaConstraint(ReferenceEntity.class, STABLE_IDENTIFIER);

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
    private static String getClassName(GKInstance instance){
        if(instance.getSchemClass().isa("Drug") && instance.getSchemClass().isValidAttribute(ReactomeJavaConstants.drugType)) {
            try {
                GKInstance drugType = (GKInstance) instance.getAttributeValue(ReactomeJavaConstants.drugType);
                return DatabaseObject.class.getPackage().getName() + "." + drugType.getDisplayName();
            } catch (Exception e) {
                return DatabaseObject.class.getPackage().getName() + "." + instance.getSchemClass().getName();
            }
        } else {
            return DatabaseObject.class.getPackage().getName() + "." + instance.getSchemClass().getName();
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
    private static Label[] getAllClassNames(Class<?> clazz) {
        List<?> superClasses = ClassUtils.getAllSuperclasses(clazz);
        List<Label> labels = new ArrayList<>();
        labels.add(Label.label(clazz.getSimpleName()));
        for (Object object : superClasses) {
            Class<?> superClass = (Class<?>) object;
            if (!superClass.equals(Object.class)) {
                labels.add(Label.label(superClass.getSimpleName()));
            }
        }
        //noinspection ToArrayCallWithZeroLengthArrayArgument
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
    private void setUpFields(Class<?> clazz) {
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
    private void addFields(Map<Class<?>, List<ReactomeAttribute>> map, Class<?> clazz, String field, boolean addedField) {
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
    @SuppressWarnings("SameParameterValue")
    private Collection<GKInstance> getCollectionFromGkInstanceReferrals(GKInstance instance, String attribute) {
        Collection<GKInstance> rtn = null;
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
                System.out.print(prefix + Math.round((++i/total) * 100) + "% (please wait...)");
                PreparedStatement css = dbaConn.prepareStatement(checkSumQuery);
                ResultSet cs = css.executeQuery();
                if(cs.next()) checkSum += cs.getLong("Checksum");
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

    private boolean isConsistent(GKInstance instance, Object value, String attribute, ReactomeAttribute.PropertyType type){
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

    private final Map<String, Map<String, Set<Long>>> consistency = new HashMap<>();
    private int consistencyLoggerEntries = 0;

    private void addConsistencyCheckEntry(String className, String attribute, ReactomeAttribute.PropertyType type, String error, Long dbId, String displayName) {
        consistency.computeIfAbsent(className, k -> new HashMap<>()).computeIfAbsent(attribute, k-> new HashSet<>()).add(dbId);
        if (consistencyLoggerEntries == 0) {
            consistencyCheckReportLogger.error("SchemaClass,Attribute,Category,Error,DbId,DisplayName");
        }
        consistencyCheckReportLogger.error(String.format("%s,%s,%s,%s,%s,\"%s\"", className, attribute, type, error, dbId, displayName));
        consistencyLoggerEntries++;
    }

    private void printConsistencyCheckReport(){
        if(consistencyLoggerEntries == 0) return;
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