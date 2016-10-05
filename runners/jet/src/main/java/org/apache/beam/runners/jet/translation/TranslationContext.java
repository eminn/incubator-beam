package org.apache.beam.runners.jet.translation;

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.Vertex;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.runners.TransformTreeNode;

public class TranslationContext {

    private ArrayList<TranslationVertex> vertices = new ArrayList<>();
    private Map<String, TranslationVertex> vertexMap = new HashMap<>();

    public TranslationContext() {
    }


    public void addVertex(Vertex vertex, TransformTreeNode node, boolean partitioned) {
        TranslationVertex translationVertex = new TranslationVertex(vertex, node.getInput().toString(), node.getOutput().toString(), partitioned);
        vertices.add(translationVertex);
        vertexMap.put(vertex.getName(), translationVertex);
    }

    public DAG buildDag() {
        System.out.println("TranslationContext.buildDag");
        DAG dag = new DAG();
        Vertex previous = null;
        for (TranslationVertex translationVertex : vertices) {
            System.out.println("translationVertex.getVertex().getName() = " + translationVertex.getVertex().getName());
            System.out.println("translationVertex.getInputName() = " + translationVertex.getInputName());
            System.out.println("translationVertex.getOutputName() = " + translationVertex.getOutputName());
            System.out.println("translationVertex.isPartitioned() = " + translationVertex.isPartitioned());
            dag.addVertex(translationVertex.getVertex());
            if (previous != null) {
                Edge edge = new Edge("edge", previous, translationVertex.getVertex());
                dag.addEdge(edge);
            }
            previous = translationVertex.getVertex();
        }
        return dag;
    }
}
