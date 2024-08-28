package com.ericsson.kyo.mdr;

public class FlowProperties {

    private String TechPackID = "";
    private String TechPackVersionID = "";
    private String FlowName = "";
    private String ActionName = "";
    private String WorkerName = "";
    private String DependencyFlow = "";
    private String DependencyWorker = "";
    private boolean Reoccuring;
    private long Interval;
    private long offset;

    public String getTechPackID() {
        return TechPackID;
    }

    public void setTechPackID(String techPackID) {
        TechPackID = techPackID;
    }

    public String getTechPackVersionID() {
        return TechPackVersionID;
    }

    public void setTechPackVersionID(String techPackVersionID) {
        TechPackVersionID = techPackVersionID;
    }

    public String getActionName() {
        return ActionName;
    }

    public void setActionName(String actionName) {
        ActionName = actionName;
    }

    public String getDependencyFlow() {
        return DependencyFlow;
    }

    public void setDependencyFlow(String dependencyFlow) {
        DependencyFlow = dependencyFlow;
    }

    public String getDependencyWorker() {
        return DependencyWorker;
    }

    public void setDependencyWorker(String dependencyWorker) {
        DependencyWorker = dependencyWorker;
    }

    public String getFlowName() {
        return FlowName;
    }

    public void setFlowName(String flowName) {
        FlowName = flowName;
    }

    public String getWorkerName() {
        return WorkerName;
    }

    public void setWorkerName(String workerName) {
        WorkerName = workerName;
    }

    public boolean isReoccuring() {
        return Reoccuring;
    }

    public void setReoccuring(boolean reoccuring) {
        Reoccuring = reoccuring;
    }

    public long getInterval() {
        return Interval;
    }

    public void setInterval(long interval) {
        Interval = interval;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
