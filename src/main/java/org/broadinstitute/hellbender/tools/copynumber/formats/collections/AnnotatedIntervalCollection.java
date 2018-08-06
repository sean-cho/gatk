package org.broadinstitute.hellbender.tools.copynumber.formats.collections;

import org.apache.commons.collections4.ListUtils;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.tools.copynumber.formats.metadata.LocatableMetadata;
import org.broadinstitute.hellbender.tools.copynumber.formats.records.AnnotatedInterval;
import org.broadinstitute.hellbender.tools.copynumber.formats.records.AnnotationCollection;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.tsv.DataLine;
import org.broadinstitute.hellbender.utils.tsv.TableColumnCollection;

import java.io.File;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Samuel Lee &lt;slee@broadinstitute.org&gt;
 */
public final class AnnotatedIntervalCollection extends AbstractLocatableCollection<LocatableMetadata, AnnotatedInterval> {
    //note to developers: repeat the column headers in Javadoc so that they are viewable when linked
    /**
     * CONTIG, START, END; columns headers for additional annotations can be specified
     */
    enum AnnotatedIntervalTableColumn {
        CONTIG,
        START,
        END;

        static final TableColumnCollection STANDARD_COLUMNS = new TableColumnCollection((Object[]) values());
    }

    enum AnnotationValueType {
        Integer,
        Long,
        Double,
        String
    }
    
    private static final BiConsumer<AnnotatedInterval, DataLine> ANNOTATED_INTERVAL_RECORD_TO_DATA_LINE_ENCODER = (annotatedInterval, dataLine) -> {
        dataLine.append(annotatedInterval.getInterval().getContig())
                .append(annotatedInterval.getInterval().getStart())
                .append(annotatedInterval.getInterval().getEnd());
        // append depending on type
        final AnnotationCollection annotations = annotatedInterval.getAnnotationCollection();
        for (final AnnotationCollection.AnnotationKey<?> key : annotations.getKeys()) {
            final AnnotationValueType type = AnnotationValueType.valueOf(key.getType().getCanonicalName());
            switch (type) {
                case Integer:
                    dataLine.append((Integer) annotations.getValue(key));
                    break;
                case Long:
                    dataLine.append((Long) annotations.getValue(key));
                    break;
                case Double:
                    dataLine.append((Double) annotations.getValue(key));
                    break;
                case String:
                    dataLine.append((String) annotations.getValue(key));
                    break;
                default:
                    throw new UserException.BadInput(String.format("Unsupported annotation type: %s", type));
            }
        }
    };

    public AnnotatedIntervalCollection(final File inputFile,
                                       final List<AnnotationCollection.AnnotationKey<?>> annotationKeys) {
        super(
                inputFile,
                getColumns(annotationKeys),
                getAnnotatedIntervalRecordFromDataLineDecoder(annotationKeys),
                ANNOTATED_INTERVAL_RECORD_TO_DATA_LINE_ENCODER);
    }

    public AnnotatedIntervalCollection(final LocatableMetadata metadata,
                                       final List<AnnotatedInterval> annotatedIntervals) {
        super(
                metadata,
                annotatedIntervals,
                getColumns(getAnnotationKeys(annotatedIntervals)),
                getAnnotatedIntervalRecordFromDataLineDecoder(getAnnotationKeys(annotatedIntervals)),
                ANNOTATED_INTERVAL_RECORD_TO_DATA_LINE_ENCODER);
    }

    private static TableColumnCollection getColumns(final List<AnnotationCollection.AnnotationKey<?>> annotationKeys) {
        return new TableColumnCollection(
                ListUtils.union(
                        AnnotatedIntervalTableColumn.STANDARD_COLUMNS.names(),
                        annotationKeys.stream().map(AnnotationCollection.AnnotationKey::getName).collect(Collectors.toList())));
    }

    private static List<AnnotationCollection.AnnotationKey<?>> getAnnotationKeys(final List<AnnotatedInterval> annotatedIntervals) {
        return annotatedIntervals.isEmpty() ? new ArrayList<>() : annotatedIntervals.get(0).getAnnotationCollection().getKeys();
    }

    private static Function<DataLine, AnnotatedInterval> getAnnotatedIntervalRecordFromDataLineDecoder(
            final List<AnnotationCollection.AnnotationKey<?>> annotationKeys) {
        return dataLine -> {
            final String contig = dataLine.get(AnnotatedIntervalTableColumn.CONTIG);
            final int start = dataLine.getInt(AnnotatedIntervalTableColumn.START);
            final int end = dataLine.getInt(AnnotatedIntervalTableColumn.END);
            final SimpleInterval interval = new SimpleInterval(contig, start, end);
            final List<Map.Entry<AnnotationCollection.AnnotationKey<?>, Object>> annotations = new ArrayList<>(annotationKeys.size());
            for (final AnnotationCollection.AnnotationKey<?> key : annotationKeys) {
                final AnnotationValueType type = AnnotationValueType.valueOf(key.getType().getCanonicalName());
                switch (type) {
                    case Integer:
                        annotations.add(new AbstractMap.SimpleEntry<>(key, dataLine.getInt(key.getName())));
                        break;
                    case Long:
                        annotations.add(new AbstractMap.SimpleEntry<>(key, dataLine.getLong(key.getName())));
                        break;
                    case Double:
                        annotations.add(new AbstractMap.SimpleEntry<>(key, dataLine.getDouble(key.getName())));
                        break;
                    case String:
                        annotations.add(new AbstractMap.SimpleEntry<>(key, dataLine.get(key.getName())));
                        break;
                    default:
                        throw new UserException.BadInput(String.format("Unsupported annotation type: %s", type));
                }
            }
            final AnnotationCollection annotationCollection = new AnnotationCollection(annotations);
            return new AnnotatedInterval(interval, annotationCollection);
        };
    }
}