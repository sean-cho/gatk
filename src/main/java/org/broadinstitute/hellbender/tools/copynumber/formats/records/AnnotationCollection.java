package org.broadinstitute.hellbender.tools.copynumber.formats.records;

import com.google.common.collect.ImmutableMap;
import org.broadinstitute.hellbender.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Represents an immutable ordered collection of named, typed annotations for an interval.
 *
 * @author Samuel Lee &lt;slee@broadinstitute.org&gt;
 */
public final class AnnotationCollection {
    public static final class AnnotationKey<T> {
        private final String name;
        private final Class<T> clazz;
        final Function<T, Boolean> validateValue;

        public AnnotationKey(final String name,
                             final Class<T> clazz,
                             final Function<T, Boolean> validateValue) {
            this.name = Utils.nonEmpty(name);
            this.clazz = Utils.nonNull(clazz);
            this.validateValue = Utils.nonNull(validateValue);
        }

        public String getName() {
            return name;
        }

        public Class<T> getType() {
            return clazz;
        }

        public void validate(final T value) {
            Utils.validateArg(validateValue.apply(value),
                    String.format("Invalid value %s for annotation %s.", value, name));
        }
    }

    private final Map<AnnotationKey<?>, Object> annotationMap;

    public AnnotationCollection(final List<Map.Entry<AnnotationKey<?>, Object>> annotations) {
        Utils.nonEmpty(annotations);
        final ImmutableMap.Builder<AnnotationKey<?>, Object> builder = new ImmutableMap.Builder<>();
        annotations.forEach(a -> builder.put(a.getKey(), a.getValue()));
        annotationMap = builder.build();
    }

    public List<AnnotationKey<?>> getKeys() {
        return new ArrayList<>(annotationMap.keySet());
    }

    public <T> T getValue(final AnnotationKey<T> key) {
        Utils.nonNull(key);
        if (!annotationMap.containsKey(key)) {
            throw new IllegalArgumentException(
                    String.format("Annotation %s not contained in AnnotationCollection.", key.name));
        }
        return key.getType().cast(annotationMap.get(key));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AnnotationCollection that = (AnnotationCollection) o;

        return annotationMap.equals(that.annotationMap);
    }

    @Override
    public int hashCode() {
        return annotationMap.hashCode();
    }

    @Override
    public String toString() {
        return "AnnotationCollection{" +
                "annotationMap=" + annotationMap +
                '}';
    }
}
