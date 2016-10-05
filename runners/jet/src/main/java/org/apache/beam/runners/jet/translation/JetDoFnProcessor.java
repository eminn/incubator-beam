package org.apache.beam.runners.jet.translation;

import com.hazelcast.jet.Processor;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.runtime.OutputCollector;
import com.hazelcast.jet.runtime.TaskContext;
import org.apache.beam.sdk.transforms.OldDoFn;

public class JetDoFnProcessor<I, O> implements Processor<Object, Object> {
    private OldDoFn<I, O> fn;
    private TaskContext taskContext;

    public JetDoFnProcessor(OldDoFn<I, O> fn) {
        this.fn = fn;
    }

    @Override
    public void before(TaskContext taskContext) {
        this.taskContext = taskContext;
    }

    @Override
    public boolean process(InputChunk<Object> input, OutputCollector<Object> output, String source) throws Exception {
        System.out.println("JetDoFnProcessor.process");
        System.out.println("input = [" + input + "], output = [" + output + "], source = [" + source + "]");

        JetProcessContext<I, O> context = new JetProcessContext<>(fn, output, taskContext);
        fn.startBundle(context);
        for (Object o : input) {
            if (o instanceof Pair){
                Pair pair = (Pair)o;
                context.inputValue = (I) pair.getValue();
            } else {
                context.inputValue = (I) o;
            }
            System.out.println("context.inputValue = " + context.inputValue);
            fn.processElement(context);

        }
        fn.finishBundle(context);
        return true;
    }

}
