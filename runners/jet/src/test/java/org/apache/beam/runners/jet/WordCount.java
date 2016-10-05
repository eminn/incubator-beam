package org.apache.beam.runners.jet;

import com.google.common.base.Strings;
import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.util.IOChannelFactory;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;


public class WordCount {

    /**
     * Concept #2: You can make your pipeline code less verbose by defining your DoFns statically out-
     * of-line. This DoFn tokenizes lines of text into individual words; we pass it to a ParDo in the
     * pipeline.
     */
    static class ExtractWordsFn extends DoFn<String, String> {
        private final Aggregator<Long, Long> emptyLines =
                createAggregator("emptyLines", new Sum.SumLongFn());

        @ProcessElement
        public void processElement(ProcessContext c) {
            if (c.element().trim().isEmpty()) {
                emptyLines.addValue(1L);
            }

            // Split the line into words.
            String[] words = c.element().split("[^a-zA-Z']+");

            // Output each word encountered into the output PCollection.
            for (String word : words) {
                if (!word.isEmpty()) {
                    c.output(word);
                }
            }
        }
    }

    /**
     * A SimpleFunction that converts a Word and Count into a printable string.
     */
    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + ": " + input.getValue();
        }
    }

    /**
     * A PTransform that converts a PCollection containing lines of text into a PCollection of
     * formatted word counts.
     * <p>
     * <p>Concept #3: This is a custom composite transform that bundles two transforms (ParDo and
     * Count) as a reusable PTransform subclass. Using composite transforms allows for easy reuse,
     * modular testing, and an improved monitoring experience.
     */
    public static class CountWords extends PTransform<PCollection<String>,
            PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> apply(PCollection<String> lines) {

            // Convert lines of text into individual words.
            PCollection<String> words = lines.apply(
                    ParDo.of(new ExtractWordsFn()));

            // Count the number of times each word occurs.
            PCollection<KV<String, Long>> wordCounts =
                    words.apply(Count.<String>perElement());

            return wordCounts;
        }
    }

    /**
     * Options supported by {@link WordCount}.
     * <p>
     * <p>Concept #4: Defining your own configuration options. Here, you can add your own arguments
     * to be processed by the command-line parser, and specify default values for them. You can then
     * access the options values in your pipeline code.
     * <p>
     * <p>Inherits standard configuration options.
     */
    public static interface WordCountOptions extends PipelineOptions {
        @Description("Path of the file to read from")
        @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Default.InstanceFactory(OutputFactory.class)
        String getOutput();

        void setOutput(String value);

        /**
         * Returns "gs://${YOUR_TEMP_DIRECTORY}/counts.txt" as the default destination.
         */
        public static class OutputFactory implements DefaultValueFactory<String> {
            @Override
            public String create(PipelineOptions options) {
                String tempLocation = options.getTempLocation();
                if (!Strings.isNullOrEmpty(tempLocation)) {
                    try {
                        IOChannelFactory factory = IOChannelUtils.getFactory(tempLocation);
                        return factory.resolve(tempLocation, "counts.txt");
                    } catch (IOException e) {
                        throw new RuntimeException(
                                String.format("Failed to resolve temp location: %s", tempLocation));
                    }
                } else {
                    throw new IllegalArgumentException("Must specify --output or --tempLocation");
                }
            }
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(JetRunner.class);
        Pipeline p = Pipeline.create(options);

        // Concepts #2 and #3: Our pipeline applies the composite CountWords transform, and passes the
        // static FormatAsTextFn() to the ParDo transform.
        p.apply("ReadLines", TextIO.Read.from("/Users/emindemirci/Desktop/SPARK"))
                .apply(new CountWords())
                .apply(MapElements.via(new FormatAsTextFn()))
                .apply("WriteCounts", TextIO.Write.to("/Users/emindemirci/Desktop/wordcount"));

        p.run();
    }
}
