package org.broadinstitute.hellbender.tools.copynumber.formats.collections;

import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.tools.copynumber.AnnotateIntervals;
import org.broadinstitute.hellbender.tools.copynumber.formats.metadata.LocatableMetadata;
import org.broadinstitute.hellbender.tools.copynumber.formats.records.AnnotatedInterval;
import org.broadinstitute.hellbender.tools.copynumber.formats.records.AnnotationMap;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.tsv.DataLine;
import org.broadinstitute.hellbender.utils.tsv.TableColumnCollection;

import java.io.File;
import java.util.*;
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
        final AnnotationMap annotations = annotatedInterval.getAnnotationMap();
        for (final AnnotationMap.AnnotationKey<?> key : annotations.getKeys()) {
            final AnnotationValueType type = AnnotationValueType.valueOf(key.getType().getSimpleName());
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

    public AnnotatedIntervalCollection(final File inputFile) {
        super(
                inputFile,
                getColumns(Collections.singletonList(AnnotateIntervals.GCContentAnnotator.ANNOTATION_KEY)),
                getAnnotatedIntervalRecordFromDataLineDecoder(Collections.singletonList(AnnotateIntervals.GCContentAnnotator.ANNOTATION_KEY)),
                ANNOTATED_INTERVAL_RECORD_TO_DATA_LINE_ENCODER);
    }

    public AnnotatedIntervalCollection(final File inputFile,
                                       final List<AnnotationMap.AnnotationKey<?>> annotationKeys) {
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

    private static TableColumnCollection getColumns(final List<AnnotationMap.AnnotationKey<?>> annotationKeys) {
        return new TableColumnCollection(
                ListUtils.union(
                        AnnotatedIntervalTableColumn.STANDARD_COLUMNS.names(),
                        annotationKeys.stream().map(AnnotationMap.AnnotationKey::getName).collect(Collectors.toList())));
    }

    private static List<AnnotationMap.AnnotationKey<?>> getAnnotationKeys(final List<AnnotatedInterval> annotatedIntervals) {
        return annotatedIntervals.isEmpty() ? new ArrayList<>() : annotatedIntervals.get(0).getAnnotationMap().getKeys();
    }

    private static Function<DataLine, AnnotatedInterval> getAnnotatedIntervalRecordFromDataLineDecoder(
            final List<AnnotationMap.AnnotationKey<?>> annotationKeys) {
        return dataLine -> {
            final String contig = dataLine.get(AnnotatedIntervalTableColumn.CONTIG);
            final int start = dataLine.getInt(AnnotatedIntervalTableColumn.START);
            final int end = dataLine.getInt(AnnotatedIntervalTableColumn.END);
            final SimpleInterval interval = new SimpleInterval(contig, start, end);
            final List<Pair<AnnotationMap.AnnotationKey<?>, Object>> annotations = new ArrayList<>(annotationKeys.size());
            for (final AnnotationMap.AnnotationKey<?> key : annotationKeys) {
                final AnnotationValueType type = AnnotationValueType.valueOf(key.getType().getSimpleName());
                switch (type) {
                    case Integer:
                        annotations.add(Pair.of(key, dataLine.getInt(key.getName())));
                        break;
                    case Long:
                        annotations.add(Pair.of(key, dataLine.getLong(key.getName())));
                        break;
                    case Double:
                        annotations.add(Pair.of(key, dataLine.getDouble(key.getName())));
                        break;
                    case String:
                        annotations.add(Pair.of(key, dataLine.get(key.getName())));
                        break;
                    default:
                        throw new UserException.BadInput(String.format("Unsupported annotation type: %s", type));
                }
            }
            final AnnotationMap annotationMap = new AnnotationMap(annotations);
            return new AnnotatedInterval(interval, annotationMap);
        };
    }
}