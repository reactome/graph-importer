package org.reactome.server.graph.batchimport;

import org.gk.model.GKInstance;

/**
 * @author Florian Korninger <florian.korninger@ebi.ac.uk>
 * @author Antonio Fabregat <fabregat@ebi.ac.uk>
 */
class GkInstancePropertiesHelper {

    private final GKInstance instance;
    private Integer count;
    private Integer order;

    GkInstancePropertiesHelper(GKInstance instance, Integer order) {
        this.instance = instance;
        this.count = 1;
        this.order = order;
    }

    GKInstance getInstance() {
        return instance;
    }

    Integer getCount() {
        return count;
    }

    Integer getOrder() {
        return order;
    }

    void increment() {
        count++;
    }
}
