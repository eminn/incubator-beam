package org.apache.beam.runners.jet.translation;

import org.apache.beam.sdk.runners.TransformTreeNode;
import org.apache.beam.sdk.transforms.PTransform;

public interface TransformTranslator<Type extends PTransform<?, ?>> {
    void translateNode(TransformTreeNode node, Type transform, TranslationContext context);
}
