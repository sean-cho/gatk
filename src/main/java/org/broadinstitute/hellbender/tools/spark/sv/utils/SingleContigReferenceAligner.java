package org.broadinstitute.hellbender.tools.spark.sv.utils;

import htsjdk.samtools.SAMFlag;
import org.apache.commons.math3.util.Pair;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.alignment.AlignmentInterval;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.bwa.BwaMemAligner;
import org.broadinstitute.hellbender.utils.bwa.BwaMemAlignment;
import org.broadinstitute.hellbender.utils.bwa.BwaMemIndex;
import org.broadinstitute.hellbender.utils.reference.FastaReferenceWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Encompasses an aligner to a single-contig reference.
 * <p>
 *     This is an {@link AutoCloseable} meant to be use in <i>try-with-resource</i> construct.
 *     If you don't do so, please remember to call {@link #close} at the end to free
 *     resources.
 * </p>
 * <p>
 *     Notice that {@link #getAligner()} provides direct access to the underlying {@link BwaMemAligner} to
 *     allow for customization if alignment options only. So please refrain from calling non-option customization
 *     methods on it directly.
 * </p>
 */
public final class SingleContigReferenceAligner implements AutoCloseable {

    private final File image;
    private final BwaMemAligner aligner;
    private final BwaMemIndex index;
    private final List<String> refNames;
    private boolean closed = false;

    /**
     * Creates an aligner given the single contig name and base sequence.
     * @param name the single contig name
     * @param bases the single contig sequence.
     *
     * @throws IllegalArgumentException if any input is {@code null} or the input bases array has
     *  length 0.
     * @throws GATKException if exception occurred when creating the corresponding index or the underlying aligner.
     */
    public SingleContigReferenceAligner(final String name, final byte[] bases) {
        Utils.nonNull(name, "the input reference name cannot be null");
        Utils.nonNull(bases, "the input bases cannot be null");
        Utils.validate(bases.length > 0, "the reference contig bases sequence must have at least one base");
        try {
            final File fasta = File.createTempFile("ssvh-temp", ".fasta");
            fasta.deleteOnExit();
            image = new File(fasta.getParentFile(), fasta.getName().replace(".fasta", ".img"));
            image.deleteOnExit();
            FastaReferenceWriter.writeSingleSequenceReference(fasta.toPath(), false, false, name, null, bases);
            BwaMemIndex.createIndexImageFromFastaFile(fasta.toString(), image.toString());
            fasta.delete(); // we don't need the fasta around.
            index = new BwaMemIndex(image.toString());
            aligner = new BwaMemAligner(index);
        } catch (final IOException | RuntimeException ex) {
            throw new GATKException("could not create index files", ex);
        }
        refNames = Collections.singletonList(name);
    }

    /**
     * Gives access to the underlying aligner so that you can modify its options.
     * <p>
     *      Please only call alignment option customization methods unless you know what you are doing.
     * </p>
     *
     * @return never {@code null}.
     */
    public BwaMemAligner getAligner() {
        return aligner;
    }

    /**
     * Compose alignments of a list of sequences against the reference.
     * @param seqs the sequences to align.
     * @return never {@code null}.
     */
    public List<List<AlignmentInterval>> align(final Iterable<byte[]> seqs) {
        Utils.nonNull(seqs, "the input sequence array cannot be null");
        final List<byte[]> inputList = Utils.stream(seqs).collect(Collectors.toList());
        checkNotClosed();
        final List<List<BwaMemAlignment>> alignments = aligner.alignSeqs(inputList);
        final List<List<AlignmentInterval>> result = new ArrayList<>(alignments.size());
        for (int i = 0; i < alignments.size(); i++) {
            final int queryLength = inputList.get(i).length;
            final List<AlignmentInterval> intervals = alignments.get(i).stream()
                    .filter(bwa -> bwa.getRefId() >= 0)
                    .filter(bwa -> SAMFlag.SECONDARY_ALIGNMENT.isUnset(bwa.getSamFlag()))
                    .map(bma -> new AlignmentInterval(bma, refNames, queryLength))
                    .collect(Collectors.toList()); // ignore secondary alignments.
            result.add(intervals);
        }
        return result;
    }

    /**
     * Compose alignments of a list of objects against the reference.
     * @param input the object to align.
     * @param inputToBytes translates an input object into a base sequence.
     * @param <T> the input object type-parameter.
     * @return never {@code null}, a list with as many elements as in the input that follows its iterable order.
     */
    public <T> List<Pair<T, List<AlignmentInterval>>> align(final Iterable<T> input, final Function<? super T, byte[]> inputToBytes) {
        Utils.nonNull(input, "the input sequence iterable cannot be null");
        Utils.nonNull(inputToBytes, "the input to base-sequence function cannot be null");

        checkNotClosed();
        final List<T> inputList = Utils.stream(input).collect(Collectors.toList());
        final int size = inputList.size();
        final List<byte[]> basesList = inputList.stream().map(inputToBytes).collect(Collectors.toList());
        final List<List<AlignmentInterval>> alignmentList = align(basesList);
        final List<Pair<T, List<AlignmentInterval>>> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            result.add(new Pair<>(inputList.get(i), alignmentList.get(i)));
        }
        return result;
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("operation not allowed once the aligner is closed");
        }
    }

    @Override
    public void close() {
        if (!closed) {
            aligner.close();
            closed = true;
            try {
                index.close();
            } finally {
                image.delete();
            }
        }
    }
}
