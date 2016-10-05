package org.apache.beam.runners.jet;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;


public class WordCountWithFlatMap {

    /**
     * A SimpleFunction that converts a Word and Count into a printable string.
     */
    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + ": " + input.getValue();
        }
    }

    public static class ParseWords extends SimpleFunction<String, Iterable<String>> {
        @Override
        public Iterable<String> apply(String input) {
            return Arrays.asList(input.split("[^a-zA-Z']+"));
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(JetRunner.class);
        Pipeline p = Pipeline.create(options);

        // Concepts #2 and #3: Our pipeline applies the composite CountWords transform, and passes the
        // static FormatAsTextFn() to the ParDo transform.
        p.apply("ReadLines", TextIO.Read.from("/Users/emindemirci/Desktop/SPARK"))
                .apply(FlatMapElements.via(new ParseWords()))
                .apply(Count.<String>perElement())
                .apply(MapElements.via(new FormatAsTextFn()))
                .apply("WriteCounts", TextIO.Write.to("/Users/emindemirci/Desktop/wordcount"));

        p.run();
    }
}
