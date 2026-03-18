package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow;

public class WorkflowObjectFactory {

    /**
     * Create an instance of {@link MAPREDUCE }
     *
     */
    public MAPREDUCE createMAPREDUCE() {
        return new MAPREDUCE();
    }

    /**
     * Create an instance of {@link WORKFLOWAPP }
     *
     */
    public WORKFLOWAPP createWORKFLOWAPP() {
        return new WORKFLOWAPP();
    }

    /**
     * Create an instance of {@link JAVA }
     *
     */
    public JAVA createJAVA() {
        return new JAVA();
    }

    /**
     * Create an instance of {@link KILL }
     *
     */
    public KILL createKILL() {
        return new KILL();
    }

    /**
     * Create an instance of {@link FS }
     *
     */
    public FS createFS() {
        return new FS();
    }

    /**
     * Create an instance of {@link SWITCH }
     *
     */
    public SWITCH createSWITCH() {
        return new SWITCH();
    }

    /**
     * Create an instance of {@link PIG }
     *
     */
    public PIG createPIG() {
        return new PIG();
    }

    /**
     * Create an instance of {@link SUBWORKFLOW }
     *
     */
    public SUBWORKFLOW createSUBWORKFLOW() {
        return new SUBWORKFLOW();
    }

}
