package org.apache.beam.runners.jet;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetEngine;
import com.hazelcast.jet.Job;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.beam.runners.jet.translation.JetPipelineTranslator;
import org.apache.beam.runners.jet.translation.TranslationContext;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * date: 9/23/16
 * author: emindemirci
 */
public class JetRunner extends PipelineRunner<JetRunnerResult> {

    private static final Logger LOG = LoggerFactory.getLogger(JetRunner.class);


    @Override
    public JetRunnerResult run(Pipeline pipeline) {
        System.out.println("JetRunner.run");
        System.out.println("pipeline = [" + pipeline + "]");
        Config config = new Config();
        config.getGroupConfig().setName("madafakaaa=jet");
        JetPipelineTranslator jetPipelineTranslator = new JetPipelineTranslator();
        pipeline.traverseTopologically(jetPipelineTranslator);
        TranslationContext context = jetPipelineTranslator.getContext();
        DAG dag = context.buildDag();
//        dag.validate();
//        Iterator<Vertex> iterator = dag.getTopologicalVertexIterator();
//        while (iterator.hasNext()){
//            Vertex next = iterator.next();
//            System.out.println("next = " + next);
//        }
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        Job job = JetEngine.getJob(hazelcastInstance, "beam-runner", dag);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Future future = job.execute();

        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        hazelcastInstance.getLifecycleService().terminate();
        return null;
    }

    public static JetRunner fromOptions(PipelineOptions options) {
        System.out.println("JetRunner.fromOptions");
        System.out.println("options = [" + options + "]");
        return new JetRunner();
    }

}
