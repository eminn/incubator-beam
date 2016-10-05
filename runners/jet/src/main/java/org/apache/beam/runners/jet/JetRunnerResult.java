package org.apache.beam.runners.jet;

import java.io.IOException;
import org.apache.beam.sdk.AggregatorRetrievalException;
import org.apache.beam.sdk.AggregatorValues;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.Aggregator;
import org.joda.time.Duration;

public class JetRunnerResult implements PipelineResult {
    @Override
    public State getState() {
        return null;
    }

    @Override
    public State cancel() throws IOException {
        return null;
    }

    @Override
    public State waitUntilFinish(Duration duration) throws IOException, InterruptedException {
        return null;
    }

    @Override
    public State waitUntilFinish() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public <T> AggregatorValues<T> getAggregatorValues(Aggregator<?, T> aggregator) throws AggregatorRetrievalException {
        return null;
    }
}
