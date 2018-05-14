package org.reactome.server.graph.utils;

import org.apache.commons.io.IOUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;

public class TaxonomyHelper {

    private static final Logger logger = LoggerFactory.getLogger("importLogger");

    private final Map<Integer, Long> taxonomyMap;

    public TaxonomyHelper(Map<Integer, Long> taxonomyMap) {
        this.taxonomyMap = taxonomyMap;
    }

    /**
     * Query Ensembl REST API in order to get the taxonomy lineage
     * and then get the parent.
     * <p>
     * Once we found the species we add it to the global map, it will
     * reduce the amount of queries to an external resource.
     *
     * @return the species
     */
    public Long getTaxonomyLineage(Integer taxId) {
        if (taxId == 1 || taxId == 0 || taxId == -1) return null;

        Long speciesId = this.taxonomyMap.get(taxId);
        if (speciesId != null) return speciesId;

        try {
            String urlString = "http://rest.ensembl.org/taxonomy/id/" + taxId;
            URL url = new URL(urlString);

            URLConnection connection = url.openConnection();
            HttpURLConnection httpConnection = (HttpURLConnection) connection;
            httpConnection.setRequestProperty("Content-Type", "application/json");

            InputStream response = httpConnection.getInputStream();
            int responseCode = httpConnection.getResponseCode();

            if (responseCode != 200) {
                if (responseCode == 429 && httpConnection.getHeaderField("Retry-After") != null) {
                    double sleepFloatingPoint = Double.valueOf(httpConnection.getHeaderField("Retry-After"));
                    double sleepMillis = 1000 * sleepFloatingPoint;
                    try {
                        Thread.sleep((long) sleepMillis);
                    } catch (InterruptedException e) {
                        logger.error(e.getMessage(), e);
                    }
                    return getTaxonomyLineage(taxId);
                }
                throw new RuntimeException("Response code was not 200. Detected response was " + responseCode);
            }

            String StringFromInputStream = IOUtils.toString(response, "UTF-8");
            JSONObject jsonObject = new JSONObject(StringFromInputStream);

            int parentTaxId = jsonObject.getJSONObject("parent").getInt("id");
            response.close();

            if (taxonomyMap.containsKey(parentTaxId)) {
                Long species = taxonomyMap.get(parentTaxId);
                taxonomyMap.put(taxId, species);
                return species;
            }
        } catch (IOException | JSONException e) {
            return null;
        }
        return null;
    }
}
