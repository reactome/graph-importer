package org.reactome.server.graph.utils;

import org.gk.model.GKInstance;
import org.gk.model.ReactomeJavaConstants;
import org.gk.pathwaylayout.DiagramGeneratorFromDB;
import org.gk.persistence.MySQLAdaptor;

import java.util.HashMap;
import java.util.Map;

public class GKInstanceHelper {

    private DiagramGeneratorFromDB diagramHelper;

    private Map<Long, GKInstance> hasDiagramCache = new HashMap<>();

    public GKInstanceHelper(MySQLAdaptor dba) {
        this.diagramHelper = new DiagramGeneratorFromDB();
        this.diagramHelper.setMySQLAdaptor(dba);
    }

    @SuppressWarnings("ConstantConditions")
    public boolean pathwayContainsProcessNode(GKInstance pathway, GKInstance processNode) {
        try {
            boolean found = false;
            for (Object o : pathway.getAttributeValuesList(ReactomeJavaConstants.hasEvent)) {
                GKInstance child = (GKInstance) o;
                found |= child.equals(processNode);
                if (found) break;
            }

            if (!found) {
                for (Object o : pathway.getAttributeValuesList(ReactomeJavaConstants.hasEvent)) {
                    GKInstance child = (GKInstance) o;
                    if (child.getSchemClass().isa(ReactomeJavaConstants.Pathway) && getHasDiagram(child) == null) {
                        found |= pathwayContainsProcessNode(child, processNode);
                        if (found) break;
                    }
                }
            }
            return found;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public GKInstance getHasDiagram(GKInstance pathway) {
        GKInstance diagram = hasDiagramCache.get(pathway.getDBID());
        if (diagram != null) return diagram;
        try {
            diagram = diagramHelper.getPathwayDiagram(pathway);
            if (diagram != null) {
                hasDiagramCache.put(pathway.getDBID(), diagram);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return diagram;
    }
}
