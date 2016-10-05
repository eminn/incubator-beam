package org.apache.beam.runners.jet.translation;

import com.hazelcast.jet.Vertex;

/**
 * date: 9/30/16
 * author: emindemirci
 */
public class TranslationVertex {

    private Vertex vertex;
    private String inputName;
    private String outputName;
    private boolean partitioned;

    public TranslationVertex(Vertex vertex, String inputName, String outputName,boolean partitioned) {
        this.vertex = vertex;
        this.inputName = inputName;
        this.outputName = outputName;
        this.partitioned = partitioned;
    }


    public Vertex getVertex() {
        return vertex;
    }

    public void setVertex(Vertex vertex) {
        this.vertex = vertex;
    }

    public String getInputName() {
        return inputName;
    }

    public void setInputName(String inputName) {
        this.inputName = inputName;
    }

    public String getOutputName() {
        return outputName;
    }

    public void setOutputName(String outputName) {
        this.outputName = outputName;
    }

    public boolean isPartitioned() {
        return partitioned;
    }

    public void setPartitioned(boolean partitioned) {
        this.partitioned = partitioned;
    }

    @Override
    public String toString() {
        return "TranslationVertex{" +
                "vertex=" + vertex +
                ", inputName='" + inputName + '\'' +
                ", outputName='" + outputName + '\'' +
                ", partitioned=" + partitioned +
                '}';
    }
}
