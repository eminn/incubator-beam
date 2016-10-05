package org.apache.beam.runners.jet.translation;

import com.hazelcast.jet.Processor;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.runtime.OutputCollector;
import com.hazelcast.jet.runtime.TaskContext;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.transforms.CombineFnBase;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.util.PerKeyCombineFnRunner;
import org.apache.beam.sdk.util.PerKeyCombineFnRunners;
import org.apache.beam.sdk.values.KV;

public class JetCombinerProcessor<I, A, O> implements Processor<Object, Object> {
    private CombineFnBase.PerKeyCombineFn<I, I, A, O> fn;
    private TaskContext taskContext;
    private Map<I, A> accumulators = new HashMap<>();

    public JetCombinerProcessor(CombineFnBase.PerKeyCombineFn<I, I, A, O> fn) {
        this.fn = fn;
    }

    @Override
    public void before(TaskContext taskContext) {
        this.taskContext = taskContext;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean process(InputChunk<Object> input, OutputCollector<Object> output, String source) throws Exception {
        System.out.println("JetCombinerProcessor.process");
        System.out.println("input = [" + input + "], output = [" + output + "], source = [" + source + "]");

        JetProcessContext processContext = new JetProcessContext(new OldDoFn<KV<I, I>, KV<O, O>>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
            }
        }, output, taskContext);
        PerKeyCombineFnRunner<I, I, A, O> combineFnRunner = PerKeyCombineFnRunners.create(fn);
        for (Object object : input) {
            I key = null;
            I value = null;
            if (object instanceof Pair) {
                Pair pair = (Pair) object;
                key = (I) pair.getKey();
                value = (I) pair.getValue();
            } else if (object instanceof KV) {
                KV kv = (KV) object;
                key = (I) kv.getKey();
                value = (I) kv.getValue();
            }
            System.out.println("combine key = " + key);
            System.out.println("combine value = " + value);
            A accumulator = accumulators.get(key);
            if (accumulator == null) {
                accumulator = (A) combineFnRunner.createAccumulator(key, processContext);
                accumulators.put(key, accumulator);
            }
            combineFnRunner.addInput(key, accumulator, value, processContext);
        }
        return true;
    }

    @Override
    public boolean complete(OutputCollector<Object> output) throws Exception {
        for (Map.Entry<I, A> entry : accumulators.entrySet()) {
            A value = entry.getValue();
            if (value instanceof long[]) {
                KV<I, Long> kv = KV.of(entry.getKey(), ((long[]) value)[0]);
                System.out.println("combine output= " + kv);
                output.collect(kv);
            } else {
                KV<I, A> kv = KV.of(entry.getKey(), value);
                System.out.println("combine output = " + kv);
                output.collect(kv);
            }
        }
        return true;
    }
}
