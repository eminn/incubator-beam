package org.apache.beam.runners.jet;

import java.io.Serializable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

/**
 * date: 10/3/16
 * author: emindemirci
 */
public class GroupByKeyTest {
    @Test
    public void testGroupByKey() {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(JetRunner.class);
        Pipeline p = Pipeline.create(options);

        PCollection<KV<String, Integer>> input =
                p.apply(TextIO.Read.from("/Users/emindemirci/Desktop/groupbykey"))
                        .apply(MapElements.via(new Map()))
                        .apply(ParDo.of(new PrintElements()));

        PCollection<KV<String, Iterable<Integer>>> output =
                input.apply(GroupByKey.<String, Integer>create());


        output.apply(ParDo.of(new PrintElements2()));


        p.run();
    }

    static class Map extends SimpleFunction<String, KV<String, Integer>> {
        @Override
        public KV<String, Integer> apply(String input) {
            String[] split = input.split(" ");
            return KV.of(split[0], Integer.valueOf(split[1]));
        }
    }


    static class PrintElements2 extends DoFn<KV<String, Iterable<Integer>>, KV<String, Iterable<Integer>>> implements Serializable {
        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println("PrintElements2.processElement");
            KV<String, Iterable<Integer>> element = c.element();
            System.out.println("element.getKey() = " + element.getKey());
            System.out.println("element.getValue() = " + element.getValue());
            c.output(element);
        }
    }

    static class PrintElements extends DoFn<KV<String, Integer>, KV<String, Integer>> implements Serializable {
        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println("PrintElements.processElement");
            KV<String, Integer> element = c.element();
            System.out.println("element.getKey() = " + element.getKey());
            System.out.println("element.getValue() = " + element.getValue());
            c.output(c.element());
        }
    }


}
