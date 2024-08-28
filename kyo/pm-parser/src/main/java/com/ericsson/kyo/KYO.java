package com.ericsson.kyo;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.kyo.dispatcher.FlowScheduler;
import com.ericsson.kyo.mdr.PostgresParserDataLoader;

public class KYO {

    private static final Logger LOGGER = LoggerFactory.getLogger(KYO.class);

    public void start() {
        try {
            LOGGER.info("KYO Startup");
            
            PostgresParserDataLoader mdr = new PostgresParserDataLoader();
            
            FlowScheduler scheduler = new FlowScheduler(mdr);
            scheduler.scheduleFlowExecution();            

        } catch (Exception e) {
            LOGGER.error("Error starting KYO", e);
        }
    }    

    public static void main(final String[] args) {
        KYO kyo = new KYO();
        kyo.start();
    }
}
