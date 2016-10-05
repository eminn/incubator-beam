package org.apache.beam.runners.jet.translation;

import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.sink.FileSink;
import com.hazelcast.jet.source.FileSource;
import com.hazelcast.jet.stream.impl.processor.EmptyProcessor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.runners.TransformTreeNode;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineFnBase;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class JetTransformTranslators {

    private static final Map<Class<? extends PTransform>, TransformTranslator> TRANSLATORS = new HashMap<>();

    static {
        TRANSLATORS.put(TextIO.Read.Bound.class, new ReadTextTranslator());
        TRANSLATORS.put(TextIO.Write.Bound.class, new WriteTextTranslator());
        TRANSLATORS.put(ParDo.Bound.class, new ParDoBoundTranslator());
        TRANSLATORS.put(GroupByKey.class, new GroupByKeyTranslator());
        TRANSLATORS.put(Combine.PerKey.class, new CombinePerKeyTranslator());

    }

    public static TransformTranslator<?> getTranslator(PTransform<?, ?> transform) {
        return TRANSLATORS.get(transform.getClass());
    }


    private static class ReadTextTranslator<T> implements TransformTranslator<TextIO.Read.Bound<T>> {
        @Override
        public void translateNode(TransformTreeNode node, TextIO.Read.Bound<T> transform, TranslationContext context) {
            System.out.println("---------");

            Vertex vertex = new Vertex(node.getFullName(), EmptyProcessor.class);
            vertex.addSource(new FileSource(transform.getFilepattern()));
            context.addVertex(vertex, node, false);

            System.out.println("ReadTextTranslator.translateNode");
            System.out.println("node = [" + node.getFullName() + "], transform = [" + transform + "]");
            System.out.println("input = " + node.getInput());
            System.out.println("output = " + node.getOutput());
            System.out.println("name = " + transform.getName());
            System.out.println("path = " + transform.getFilepattern());

        }
    }

    private static class WriteTextTranslator<T> implements TransformTranslator<TextIO.Write.Bound<T>> {
        @Override
        public void translateNode(TransformTreeNode node, TextIO.Write.Bound<T> transform, TranslationContext context) {

            System.out.println("---------");

            Vertex vertex = new Vertex(node.getFullName(), EmptyProcessor.class);
            vertex.addSink(new FileSink(transform.getFilenamePrefix() + transform.getFilenameSuffix()));
            context.addVertex(vertex, node, false);

            System.out.println("WriteTextTranslator.translateNode");
            System.out.println("node = [" + node.getFullName() + "], transform = [" + transform + "]");
            System.out.println("input = " + node.getInput());
            System.out.println("output = " + node.getOutput());
            System.out.println("name = " + transform.getName());
            System.out.println("prefix = " + transform.getFilenamePrefix());
            System.out.println("suffix= " + transform.getFilenameSuffix());

        }

    }

    private static class ParDoBoundTranslator<I, O> implements TransformTranslator<ParDo.Bound<I, O>> {
        @Override
        public void translateNode(TransformTreeNode node, ParDo.Bound<I, O> transform, TranslationContext context) {
            OldDoFn<I, O> fn = transform.getFn();
            Vertex vertex = new Vertex(node.getFullName(), JetDoFnProcessor.class, fn);
            context.addVertex(vertex, node, false);

            System.out.println("---------");
            System.out.println("ParDoBoundTranslator.translateNode");
            System.out.println("node = [" + node.getFullName() + "], transform = [" + transform + "], context = [" + context + "]");
            System.out.println("input = " + node.getInput());
            System.out.println("output = " + node.getOutput());
            System.out.println("name = " + transform.getName());
            System.out.println("sideInputs = " + transform.getSideInputs());


        }
    }

    private static class GroupByKeyTranslator<K, V> implements TransformTranslator<GroupByKey<K, V>> {
        @Override
        public void translateNode(TransformTreeNode node, GroupByKey<K, V> transform, TranslationContext context) {
            Combine.KeyedCombineFn<K, V, List<V>, List<V>> combineFn =
                    new Concatenate<V>().asKeyedFn();
            Vertex vertex = new Vertex(node.getFullName(), JetCombinerProcessor.class, combineFn);
            context.addVertex(vertex, node, true);


            System.out.println("---------");
            System.out.println("GroupByKeyTranslator.translateNode");
            System.out.println("node = [" + node.getFullName() + "], transform = [" + transform + "], context = [" + context + "]");
            System.out.println("input = " + node.getInput());
            System.out.println("output = " + node.getOutput());
            System.out.println("name = " + transform.getName());
        }
    }

    /**
     * Combiner that combines {@code T}s into a single {@code List<T>} containing all inputs.
     * <p>
     * <p>For internal use to translate {@link GroupByKey}. For a large {@link PCollection
     * } this
     * is expected to crash!
     * <p>
     * <p>This is copied from the dataflow runner code.
     *
     * @param <T> the type of elements to concatenate.
     */
    private static class Concatenate<T> extends Combine.CombineFn<T, List<T>, List<T>> {
        @Override
        public List<T> createAccumulator() {
            return new ArrayList<>();
        }

        @Override
        public List<T> addInput(List<T> accumulator, T input) {
            accumulator.add(input);
            return accumulator;
        }

        @Override
        public List<T> mergeAccumulators(Iterable<List<T>> accumulators) {
            List<T> result = createAccumulator();
            for (List<T> accumulator : accumulators) {
                result.addAll(accumulator);
            }
            return result;
        }

        @Override
        public List<T> extractOutput(List<T> accumulator) {
            return accumulator;
        }

        @Override
        public Coder<List<T>> getAccumulatorCoder(CoderRegistry registry, Coder<T> inputCoder) {
            return ListCoder.of(inputCoder);
        }

        @Override
        public Coder<List<T>> getDefaultOutputCoder(CoderRegistry registry, Coder<T> inputCoder) {
            return ListCoder.of(inputCoder);
        }
    }

    private static class CombinePerKeyTranslator<K, InputT, AccumT, OutputT> implements TransformTranslator<Combine.PerKey<K, InputT, OutputT>> {
        @SuppressWarnings("unchecked")
        @Override
        public void translateNode(TransformTreeNode node, Combine.PerKey<K, InputT, OutputT> transform, TranslationContext context) {
            CombineFnBase.PerKeyCombineFn<K, InputT, AccumT, OutputT> fn = (CombineFnBase.PerKeyCombineFn<K, InputT, AccumT, OutputT>) transform.getFn();
            Vertex vertex = new Vertex(node.getFullName(), JetCombinerProcessor.class, fn);
            context.addVertex(vertex, node, true);

            System.out.println("---------");
            System.out.println("CombinePerKeyTranslator.translateNode");
            System.out.println("node = [" + node.getFullName() + "], transform = [" + transform + "], context = [" + context + "]");
            System.out.println("input = " + node.getInput());
            System.out.println("output = " + node.getOutput());
            System.out.println("name = " + transform.getName());

        }
    }
}
