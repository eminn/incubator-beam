package org.apache.beam.runners.jet.translation;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformTreeNode;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * date: 9/23/16
 * author: emindemirci
 */
public class JetPipelineTranslator extends Pipeline.PipelineVisitor.Defaults {

    private TranslationContext context;

    public JetPipelineTranslator() {
        context = new TranslationContext();
    }

    public TranslationContext getContext() {
        return context;
    }

    @Override
    public CompositeBehavior enterCompositeTransform(TransformTreeNode node) {
        System.out.println("JetPipelineTranslator.enterCompositeTransform");
        System.out.println("node = [" + node.getFullName() + "]");
        TransformTranslator<?> translator = getTranslator(node);
        if (translator == null) {
            return CompositeBehavior.ENTER_TRANSFORM;

        } else {
            transform(node.getTransform(), node, translator);
            return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
        }
    }

    @Override
    public void leaveCompositeTransform(TransformTreeNode node) {
        System.out.println("JetPipelineTranslator.leaveCompositeTransform");
        System.out.println("node = [" + node.getFullName() + "]");
        super.leaveCompositeTransform(node);
    }

    @Override
    public void visitPrimitiveTransform(TransformTreeNode node) {
        System.out.println("JetPipelineTranslator.visitPrimitiveTransform");
        System.out.println("node = [" + node.getFullName() + "]");
        PTransform<?, ?> transform = node.getTransform();
        System.out.println("transform.getClass() = " + transform.getClass());

        TransformTranslator<?> translator = JetTransformTranslators.getTranslator(transform);
        if (translator == null) {
            throw new UnsupportedOperationException(node.getTransform().getName());
        }
        transform(transform, node, translator);
    }

    private <T extends PTransform<?, ?>> void transform(PTransform<?, ?> transform, TransformTreeNode node, TransformTranslator<?> translator) {
        @SuppressWarnings("unchecked") T typedTransform = (T) transform;
        @SuppressWarnings("unchecked") TransformTranslator<T> typedTranslator = (TransformTranslator<T>) translator;
        typedTranslator.translateNode(node, typedTransform, context);
    }

    private static TransformTranslator<?> getTranslator(TransformTreeNode node) {
        PTransform<?, ?> transform = node.getTransform();
        if (transform == null) {
            return null;
        }
        return JetTransformTranslators.getTranslator(transform);
    }

}

