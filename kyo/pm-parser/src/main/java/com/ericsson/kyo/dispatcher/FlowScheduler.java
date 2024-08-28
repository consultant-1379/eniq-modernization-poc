package com.ericsson.kyo.dispatcher;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.kyo.mdr.PostgresParserDataLoader;
import com.ericsson.kyo.mdr.FlowProperties;

public class FlowScheduler {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(FlowScheduler.class);
    private PostgresParserDataLoader mdr;

    public FlowScheduler(PostgresParserDataLoader mdr) {
        this.mdr = mdr;
    }

    public void scheduleFlowExecution() {
        ArrayList<FlowProperties> scheduling = mdr.getSchedulingRequirements();

        ScheduledExecutorService ses = Executors
                .newScheduledThreadPool(scheduling.size());

        for (FlowProperties schedule : scheduling) {
            Dispatcher dispatcher = new Dispatcher(mdr, schedule);
            if (schedule.isReoccuring()) {
                long initialDelay = calculateInitialDelay(
                        schedule.getInterval(), schedule.getOffset());
                LOGGER.info(
                        "Recurring flow scheduled every {} minutes, Triggers in {} seconds",
                        schedule.getInterval(), (initialDelay / 1000));
                ses.scheduleAtFixedRate(dispatcher, initialDelay,
                        schedule.getInterval() * 60 * 1000,
                        TimeUnit.MILLISECONDS);

            } else {
                //ses.schedule(dispatcher, delay, TimeUnit.MILLISECONDS);
            }
        }
    }

    private long calculateInitialDelay(long intervalInMinutes, long offset) {
        long initialDelay = 0;
        long minutes = Calendar.getInstance().get(Calendar.MINUTE);
        minutes = (minutes / intervalInMinutes) * intervalInMinutes
                + intervalInMinutes;

        initialDelay = minutes - Calendar.getInstance().get(Calendar.MINUTE);

        if (initialDelay >= 60) {
            int hoursValue = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
            double fraction = (hoursValue * 60.0) / intervalInMinutes;
            double difference = (hoursValue * 60) / intervalInMinutes;
            fraction = fraction - difference;
            initialDelay = initialDelay
                    - Math.round(intervalInMinutes * fraction);
        }

        initialDelay = initialDelay * 60; //convert the time to seconds for better accuracy
        initialDelay = initialDelay
                - Calendar.getInstance().get(Calendar.SECOND);
        initialDelay = initialDelay + offset;
        initialDelay = TimeUnit.SECONDS.toMillis(initialDelay);
        return initialDelay;
    }

}
