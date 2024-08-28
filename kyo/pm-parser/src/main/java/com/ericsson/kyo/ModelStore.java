package com.ericsson.kyo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModelStore {

    private static ModelStore obj;
    private static int instance;
    private HashMap<String, String> queries;
    private static final Logger log = LoggerFactory.getLogger(ModelStore.class);

    private ModelStore() {
        obj = this;
        queries = new HashMap<String, String>();
        loadQueries();
        log.info("ModelStore Initialized");
    }

    public HashMap<String, String> getQueries() {
        return queries;
    }

    public void loadQueries() {
        try (BufferedReader br = new BufferedReader(
                new FileReader(KYOConstants.queries_file))) {
            for (String line; (line = br.readLine()) != null;) {
                String[] line_parts = line.split("::");
                queries.put(line_parts[0].trim(), line_parts[1].trim());

            }
        } catch (Exception e) {
            log.error("Unable to load queries", e);
        }
    }

    public static ModelStore getInstance() {
        if (instance == 0) {
            instance = 1;
            return new ModelStore();
        }
        if (instance == 1) {
            return obj;
        }

        return null;
    }

}
