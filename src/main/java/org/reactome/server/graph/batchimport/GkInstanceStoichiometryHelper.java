package org.reactome.server.graph.batchimport;

import org.gk.model.GKInstance;

/**
 * @author Florian Korninger (florian.korninger@ebi.ac.uk)
 */
class GkInstanceStoichiometryHelper {

    private final GKInstance instance;
    private Integer count;

    GkInstanceStoichiometryHelper(GKInstance instance) {
        this.instance = instance;
        this.count = 1;
    }

    GKInstance getInstance() {
        return instance;
    }

    Integer getCount() {
        return count;
    }

    void increment() {
        count++;
    }
}
