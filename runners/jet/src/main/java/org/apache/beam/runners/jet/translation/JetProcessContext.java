package org.apache.beam.runners.jet.translation;

import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.runtime.JetPair;
import com.hazelcast.jet.runtime.OutputCollector;
import com.hazelcast.jet.runtime.TaskContext;
import org.apache.beam.runners.jet.wrapper.AggregationWrapper;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowingInternals;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

public class JetProcessContext<I, O> extends OldDoFn<I, O>.ProcessContext {

    private final OutputCollector<Object> collector;
    private final TaskContext taskContext;
    I inputValue;

    JetProcessContext(OldDoFn<I, O> fn, OutputCollector<Object> collector, TaskContext taskContext) {
        fn.super();
        this.collector = collector;
        this.taskContext = taskContext;
        super.setupDelegateAggregators();
    }

    @Override
    public I element() {
        return inputValue;
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
        return null;
    }

    @Override
    public Instant timestamp() {
        return null;
    }

    @Override
    public BoundedWindow window() {
        return null;
    }

    @Override
    public PaneInfo pane() {
        return null;
    }

    @Override
    public WindowingInternals<I, O> windowingInternals() {
        return null;
    }

    @Override
    public PipelineOptions getPipelineOptions() {
        return null;
    }

    @Override
    public void output(O output) {
        if (output instanceof Pair) {
            Pair pair = (Pair) output;
            System.out.println("output pair= " + output);
            collector.collect(pair);
        } else if (output instanceof KV) {
            KV kv = (KV) output;
            System.out.println("output kv= " + output);
            collector.collect(kv);
        } else {
            System.out.println("output jetpair= " + output);
            collector.collect(new JetPair<>(output, output));
        }
    }

    @Override
    public void outputWithTimestamp(O output, Instant timestamp) {

    }

    @Override
    public <T> void sideOutput(TupleTag<T> tag, T output) {

    }

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {

    }

    @Override
    protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregatorInternal(String name, Combine.CombineFn<AggInputT, ?, AggOutputT> combiner) {
        AggregationWrapper<AggInputT, AggOutputT> wrapper = new AggregationWrapper<>(combiner);
        taskContext.setAccumulator(name, wrapper);
        return wrapper;
    }
}
