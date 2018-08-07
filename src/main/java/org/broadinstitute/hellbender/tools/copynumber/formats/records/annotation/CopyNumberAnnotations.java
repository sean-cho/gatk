package org.broadinstitute.hellbender.tools.copynumber.formats.records.annotation;

public final class CopyNumberAnnotations {
    public static AnnotationKey<Double> GC_CONTENT = new AnnotationKey<>(
            "GC_CONTENT",
            Double.class,
            gcContent -> (0. <= gcContent && gcContent <= 1.) || Double.isNaN(gcContent));

    public static AnnotationKey<Double> MAPPABILITY = new AnnotationKey<>(
            "MAPPABILITY",
            Double.class,
            mappability -> (0. <= mappability && mappability <= 1.) || Double.isNaN(mappability));
}
