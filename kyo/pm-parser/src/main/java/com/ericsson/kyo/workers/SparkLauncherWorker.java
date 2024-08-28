/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2012
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.kyo.workers;

import com.ericsson.kyo.mdr.workers.WorkerBase;
import com.ericsson.kyo.spark.PmParserSparkLauncher;

public class SparkLauncherWorker extends WorkerBase {
    
    private PmParserSparkLauncher launcher = new PmParserSparkLauncher();

    @Override
    public void execute() {
    	String SparkLauncherMainClass = configuration.getProperty("SparkLauncherMainClass");
        launcher.launchParser("1", props.getTechPackID(), props.getFlowName(), 
        		props.getWorkerName(), props.getDependencyWorker(), SparkLauncherMainClass);
    }
}