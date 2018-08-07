package org.broadinstitute.hellbender.tools.copynumber;

import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.util.Locatable;
import org.apache.commons.lang3.tuple.Pair;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.BetaFeature;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.CopyNumberProgramGroup;
import org.broadinstitute.hellbender.engine.*;
import org.broadinstitute.hellbender.tools.copynumber.arguments.CopyNumberArgumentValidationUtils;
import org.broadinstitute.hellbender.tools.copynumber.formats.collections.AnnotatedIntervalCollection;
import org.broadinstitute.hellbender.tools.copynumber.formats.metadata.SimpleLocatableMetadata;
import org.broadinstitute.hellbender.tools.copynumber.formats.records.AnnotatedInterval;
import org.broadinstitute.hellbender.tools.copynumber.formats.records.annotation.AnnotationKey;
import org.broadinstitute.hellbender.tools.copynumber.formats.records.annotation.AnnotationMap;
import org.broadinstitute.hellbender.utils.IntervalMergingRule;
import org.broadinstitute.hellbender.utils.Nucleotide;
import org.broadinstitute.hellbender.utils.SimpleInterval;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Annotates intervals with GC content and (optionally) mappability.  The output may optionally be used as input to
 * {@link CreateReadCountPanelOfNormals}, {@link DenoiseReadCounts}, and {@link GermlineCNVCaller}.  In the case,
 * using the resulting panel as input to {@link DenoiseReadCounts} will perform explicit GC-bias correction.
 *
 * <h3>Inputs</h3>
 *
 * <ul>
 *     <li>
 *         Reference FASTA file
 *     </li>
 *     <li>
 *         Intervals to be annotated. Supported formats are described in
 *         <a href ="https://software.broadinstitute.org/gatk/documentation/article?id=1319">Article#1319</a>.
 *         The argument {@code interval-merging-rule} must be set to {@link IntervalMergingRule#OVERLAPPING_ONLY}
 *         and all other common arguments for interval padding or merging must be set to their defaults.
 *     </li>
 * </ul>
 *
 * <h3>Output</h3>
 *
 * <ul>
 *     <li>
 *         GC-content annotated-intervals file.
 *         This is a tab-separated values (TSV) file with a SAM-style header containing a sequence dictionary,
 *         a row specifying the column headers contained in {@link AnnotatedIntervalCollection.AnnotatedIntervalTableColumn},
 *         and the corresponding entry rows.
 *     </li>
 * </ul>
 *
 * <h3>Usage example</h3>
 *
 * <pre>
 *     gatk AnnotateIntervals \
 *          -R reference.fa \
 *          -L intervals.interval_list \
 *          --interval-merging-rule OVERLAPPING_ONLY \
 *          -O annotated_intervals.tsv
 * </pre>
 *
 * @author David Benjamin &lt;davidben@broadinstitute.org&gt;
 * @author Samuel Lee &lt;slee@broadinstitute.org&gt;
 */
@CommandLineProgramProperties(
        summary = "Annotates intervals with GC content",
        oneLineSummary = "Annotates intervals with GC content",
        programGroup = CopyNumberProgramGroup.class
)
@DocumentedFeature
@BetaFeature
public final class AnnotateIntervals extends GATKTool {
    @Argument(
            doc = "Output file for annotated intervals.",
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
            shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME
    )
    private File outputAnnotatedIntervalsFile;

    @Override
    public boolean requiresReference() {
        return true;
    }

    @Override
    public boolean requiresIntervals() {
        return true;
    }

    private List<SimpleInterval> intervals;
    private SAMSequenceDictionary sequenceDictionary;
    private ReferenceDataSource reference;
    private List<IntervalAnnotator<?>> annotators = new ArrayList<>();
    private AnnotatedIntervalCollection annotatedIntervals;

    @Override
    public void onTraversalStart() {
        CopyNumberArgumentValidationUtils.validateIntervalArgumentCollection(intervalArgumentCollection);

        logger.info("Loading intervals for annotation...");
        sequenceDictionary = getBestAvailableSequenceDictionary();
        intervals = intervalArgumentCollection.getIntervals(sequenceDictionary);

        logger.info("Loading resources for annotation...");
        reference = ReferenceDataSource.of(referenceArguments.getReferencePath());  //the GATKTool ReferenceDataSource is package-protected, so we cannot access it directly

        annotators.add(new GCContentAnnotator());

        logger.info("Annotating intervals...");
    }

    @Override
    public void traverse() {
        final List<AnnotatedInterval> annotatedIntervalList = new ArrayList<>(intervals.size());
        for (final SimpleInterval interval : intervals) {
            final ReferenceContext referenceContext = new ReferenceContext(reference, interval);
            final AnnotationMap annotations = new AnnotationMap(annotators.stream()
                    .collect(Collectors.mapping(
                            a -> Pair.of(
                                    a.getAnnotationKey(),
                                    a.apply(interval, null, referenceContext, null)),
                            Collectors.toList())));
            annotatedIntervalList.add(new AnnotatedInterval(interval, annotations));
            progressMeter.update(interval);
        }
        annotatedIntervals = new AnnotatedIntervalCollection(new SimpleLocatableMetadata(sequenceDictionary), annotatedIntervalList);
    }

    @Override
    public Object onTraversalSuccess() {
        logger.info(String.format("Writing annotated intervals to %s...", outputAnnotatedIntervalsFile));
        annotatedIntervals.write(outputAnnotatedIntervalsFile);
        return super.onTraversalSuccess();
    }

    /**
     * If additional annotators are added to this tool, they should follow this interface.
     * Validation that the required resources are available should be performed before
     * calling {@link IntervalAnnotator#apply}.
     */
    interface IntervalAnnotator<T> {
        AnnotationKey<T> getAnnotationKey();

        /**
         * The returned value should be validated using {@link AnnotationKey#validate}.
         */
        T apply(final Locatable interval,
                final ReadsContext readsContext,
                final ReferenceContext referenceContext,
                final FeatureContext featureContext);
    }

    public static class GCContentAnnotator implements IntervalAnnotator<Double> {
        public static final AnnotationKey<Double> ANNOTATION_KEY = new AnnotationKey<>(
                "GC_CONTENT",
                Double.class,
                gcContent -> (0. <= gcContent && gcContent <= 1.) || Double.isNaN(gcContent));

        @Override
        public AnnotationKey<Double> getAnnotationKey() {
            return ANNOTATION_KEY;
        }

        @Override
        public Double apply(final Locatable interval,
                            final ReadsContext readsContext,
                            final ReferenceContext referenceContext,
                            final FeatureContext featureContext) {
            final Nucleotide.Counter counter = new Nucleotide.Counter();
            counter.addAll(referenceContext.getBases());
            final long gcCount = counter.get(Nucleotide.C) + counter.get(Nucleotide.G);
            final long atCount = counter.get(Nucleotide.A) + counter.get(Nucleotide.T);
            final long totalCount = gcCount + atCount;
            final double gcContent = totalCount == 0 ? Double.NaN : gcCount / (double) totalCount;
            return getAnnotationKey().validate(gcContent);
        }
    }

    public static class MappabilityAnnotator implements IntervalAnnotator<Double> {
        @Override
        public AnnotationKey<Double> getAnnotationKey() {
            return new AnnotationKey<>(
                    "MAPPABILITY",
                    Double.class,
                    mappability -> (0. <= mappability && mappability <= 1.) || Double.isNaN(mappability));
        }

        @Override
        public Double apply(final Locatable interval,
                            final ReadsContext readsContext,
                            final ReferenceContext referenceContext,
                            final FeatureContext featureContext) {
            // TODO
            return Double.NaN;
        }
    }
}
