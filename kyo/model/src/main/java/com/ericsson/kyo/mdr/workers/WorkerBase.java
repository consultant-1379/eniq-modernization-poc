package com.ericsson.kyo.mdr.workers;

import java.util.Properties;

import com.ericsson.kyo.mdr.FlowProperties;
import com.ericsson.kyo.mdr.PostgresParserDataLoader;

public abstract class WorkerBase {

    protected Properties configuration;
    protected FlowProperties props;
    protected PostgresParserDataLoader mdr;

    public void setConfiguration(FlowProperties props, Properties configuration,
            PostgresParserDataLoader mdr) {
        this.props = props;
        this.configuration = configuration;
        this.mdr = mdr;
    }

    public abstract void execute();

}
