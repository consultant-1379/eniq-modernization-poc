package com.ericsson.kyo.dispatcher;

import java.util.ArrayList;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.kyo.mdr.PostgresParserDataLoader;
import com.ericsson.kyo.mdr.workers.WorkerBase;

import com.ericsson.kyo.mdr.FlowProperties;

public class Dispatcher implements Runnable {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(Dispatcher.class);
    private PostgresParserDataLoader mdr;
    private FlowProperties flowprops;

    public Dispatcher(PostgresParserDataLoader mdr, FlowProperties flowprops) {
        this.mdr = mdr;
        this.flowprops = flowprops;
    }

    public void run() {
        try {
            Properties configuration = mdr.getWorkerConfig(
                    flowprops.getTechPackID(), flowprops.getFlowName(),
                    flowprops.getWorkerName());
            executeWorker(flowprops, configuration);

            ArrayList<FlowProperties> dependencies = mdr.getDependentWorker(
                    flowprops.getTechPackID(), flowprops.getFlowName(),
                    flowprops.getActionName());
            handleDependencies(dependencies);
        } catch (Exception e) {
            LOGGER.error("Unable to execute worker. ", e);
        }
    }

    public void handleDependencies(ArrayList<FlowProperties> dependencies)
            throws InstantiationException, IllegalAccessException,
            ClassNotFoundException {
        if (dependencies.size() > 0) {
            for (FlowProperties props : dependencies) {
                Properties configuration = mdr.getWorkerConfig(
                        props.getTechPackID(), props.getFlowName(),
                        props.getWorkerName());
                executeWorker(props, configuration);

                ArrayList<FlowProperties> deps = mdr.getDependentWorker(
                        props.getTechPackID(), props.getFlowName(),
                        props.getActionName());
                handleDependencies(deps);
            }
        }
    }

    public void executeWorker(FlowProperties props, Properties configuration)
            throws InstantiationException, IllegalAccessException,
            ClassNotFoundException {
        WorkerBase worker = (WorkerBase) Class.forName(props.getWorkerName())
                .newInstance();
        worker.setConfiguration(props, configuration, mdr);
        LOGGER.info("Executing {}", worker.getClass().getName());
        worker.execute();
    }
}
