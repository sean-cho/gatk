package org.broadinstitute.hellbender.tools.copynumber.formats.collections;

import org.broadinstitute.hellbender.tools.copynumber.formats.metadata.LocatableMetadata;
import org.broadinstitute.hellbender.tools.copynumber.formats.records.AnnotatedInterval;
import org.broadinstitute.hellbender.tools.copynumber.formats.records.AnnotationCollection;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.tsv.DataLine;
import org.broadinstitute.hellbender.utils.tsv.TableColumnCollection;

import java.io.File;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * @author Samuel Lee &lt;slee@broadinstitute.org&gt;
 */
public final class AnnotatedIntervalCollection extends AbstractLocatableCollection<LocatableMetadata, AnnotatedInterval> {
    //note to developers: repeat the column headers in Javadoc so that they are viewable when linked
    /**
     * CONTIG, START, END, and columns headers given by {@link AnnotationCollection}
     */
    enum AnnotatedIntervalTableColumn {
        CONTIG,
        START,
        END;

        static final TableColumnCollection STANDARD_COLUMNS = new TableColumnCollection((Object[]) values());
    }
    
    private static final Function<DataLine, AnnotatedInterval> ANNOTATED_INTERVAL_RECORD_FROM_DATA_LINE_DECODER = dataLine -> {
        final String contig = dataLine.get(AnnotatedIntervalTableColumn.CONTIG);
        final int start = dataLine.getInt(AnnotatedIntervalTableColumn.START);
        final int end = dataLine.getInt(AnnotatedIntervalTableColumn.END);
        final SimpleInterval interval = new SimpleInterval(contig, start, end);
        final AnnotationCollection annotationCollection = new AnnotationCollection(dataLine);
        return new AnnotatedInterval(interval, annotationCollection);
    };

    private static final BiConsumer<AnnotatedInterval, DataLine> ANNOTATED_INTERVAL_RECORD_TO_DATA_LINE_ENCODER = (annotatedInterval, dataLine) -> {
        dataLine.append(annotatedInterval.getInterval().getContig())
                .append(annotatedInterval.getInterval().getStart())
                .append(annotatedInterval.getInterval().getEnd());
        // append depending on type
        final AnnotationCollection annotations = annotatedInterval.getAnnotationCollection();
        annotations.getKeys().forEach(k -> dataLine.append(annotations.getValue(k)));
    };

    public AnnotatedIntervalCollection(final File inputFile,
                                       final List<AnnotationKey<?>> optionalAnnotationKeys) {
        super(inputFile, AnnotatedIntervalCollection.AnnotatedIntervalTableColumn.COLUMNS, ANNOTATED_INTERVAL_RECORD_FROM_DATA_LINE_DECODER, ANNOTATED_INTERVAL_RECORD_TO_DATA_LINE_ENCODER);
    }

    public AnnotatedIntervalCollection(final LocatableMetadata metadata,
                                       final List<AnnotatedInterval> annotatedIntervals) {
        super(metadata, annotatedIntervals, AnnotatedIntervalCollection.AnnotatedIntervalTableColumn.COLUMNS, ANNOTATED_INTERVAL_RECORD_FROM_DATA_LINE_DECODER, ANNOTATED_INTERVAL_RECORD_TO_DATA_LINE_ENCODER);
    }

    private static TableColumnCollection construct
}
