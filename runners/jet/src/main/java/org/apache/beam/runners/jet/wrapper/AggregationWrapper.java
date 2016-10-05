package org.apache.beam.runners.jet.wrapper;

import com.hazelcast.jet.counters.Accumulator;
import java.io.Serializable;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;

public class AggregationWrapper <InputT, OutputT>
        implements Aggregator<InputT, OutputT>, Accumulator<InputT, Serializable> {
    public AggregationWrapper(Combine.CombineFn<InputT, ?, OutputT> combiner) {


    }

    @Override
    public void add(InputT value) {

    }

    @Override
    public Serializable getLocalValue() {
        return null;
    }

    @Override
    public void resetLocal() {

    }

    @Override
    public void merge(Accumulator<InputT, Serializable> other) {

    }

    @Override
    public void addValue(InputT value) {

    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public Combine.CombineFn<InputT, ?, OutputT> getCombineFn() {
        return null;
    }
}
