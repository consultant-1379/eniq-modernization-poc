package com.ericsson.kyo.workers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.kyo.mdr.workers.WorkerBase;

public class FileCollectionWorker extends WorkerBase {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(FileCollectionWorker.class);
    private static final String FILE_PREFIX = "file://";

    public void execute() {
        LOGGER.info("Executing FileCollectionWorker");
        String inputdirectory = configuration.getProperty("inputDirectory");
        String received_date = getCurrentDate();

        try {
            final Set<String> alreadyparsed = new HashSet<>(
                    mdr.getPreviouslyParsedList(props.getTechPackID(),
                            props.getFlowName(), props.getActionName()));
            
            final List<String> waitingToParse = mdr.getFileBacklog(props.getTechPackID(),
                    props.getFlowName(), props.getActionName());
                        
            final Set<String> waitingToParseCopy = new HashSet<String>(waitingToParse);
            
            LOGGER.info("Retrieved parsed files");
            final Set<String> files = Files
                    .find(Paths.get(inputdirectory), Integer.MAX_VALUE,
                            (path, attr) -> attr.isRegularFile()
                                    && Files.isReadable(path))
                   .map(p -> FILE_PREFIX + p.toString())
                    .filter(f -> f.endsWith(".xml"))
                           .collect(Collectors.toSet());
            
            LOGGER.info("Completed listing {} files", files.size());
            
            files.removeAll(alreadyparsed); //Files in directory - Files Parsed before = "New" Files
            
            waitingToParse.removeAll(files); //Files waiting to parse - "New" Files = Files that no longer exist in the directory
            mdr.updateFileBacklogError(props.getTechPackID(), props.getFlowName(), mergeColumns(waitingToParse), received_date); //Set their status to 'Failed'
            
            files.removeAll(waitingToParseCopy); //Files in directory - Files already in the parse backlog = THE New Files
            
            mdr.addToFileBacklog(props.getTechPackID(), props.getFlowName(),
                    props.getActionName(), files, received_date);
            
            LOGGER.info("Added {} files to backlog", files.size());
            
        } catch (IOException e) {
            LOGGER.warn("Error listing files from {}", inputdirectory, e);
        }
        LOGGER.info("FileCollectionWorker Complete");
    }

    public String mergeColumns(Collection<String> columns) {
        final String sql = String.join("','", columns);
        return sql.length() > 0 ? "'" + sql + "'" : "";
    }
    
    private String getCurrentDate() {
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat dateformat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
        return dateformat.format(cal.getTime());
    }
}
