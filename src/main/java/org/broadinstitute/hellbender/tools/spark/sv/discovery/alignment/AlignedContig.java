package org.broadinstitute.hellbender.tools.spark.sv.discovery.alignment;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import htsjdk.samtools.Cigar;
import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMTag;
import htsjdk.samtools.util.SequenceUtil;
import it.unimi.dsi.fastutil.ints.Int2ByteFunction;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.broadinstitute.hellbender.engine.datasources.ReferenceMultiSource;
import org.broadinstitute.hellbender.engine.datasources.ReferenceSource;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVHaplotype;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.Nucleotide;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.Utils;
import scala.Tuple2;
import org.broadinstitute.hellbender.utils.read.CigarUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Locally assembled contig:
 *   its name
 *   its sequence as produced by the assembler (no reverse complement like in the SAM record if it maps to '-' strand), and
 *   its stripped-down alignment information.
 */
@DefaultSerializer(AlignedContig.Serializer.class)
public final class AlignedContig {

    private final String contigName;
    private final byte[] contigSequence;
    private final List<AlignmentInterval> alignmentIntervals;

    public AlignedContig(final GATKRead read) {
        Utils.nonNull(read);
        if (read.isUnmapped()) {
            throw new IllegalArgumentException("the input read cannot be unmapped");
        } else if (read.getCigar().isEmpty() || read.getCigar().containsOperator(CigarOperator.H)) {
            throw new IllegalArgumentException("the input read must have a cigar and cannot have hard-clips: " + read.getName() + " " + read.getCigar() + " " + read.getBases().length);
        } else {
            final byte[] bases = read.getBases();
            if (read.isReverseStrand()) {
                SequenceUtil.reverseComplement(bases);
            }
            final List<AlignmentInterval> intervals = new ArrayList<>();
            intervals.add(new AlignmentInterval(read));
            if (read.hasAttribute(SAMTag.SA.name())) {
                Arrays.stream(read.getAttributeAsString(SAMTag.SA.name()).split(";"))
                        .filter(s -> !s.isEmpty() && !s.equals("*"))
                        .map(AlignmentInterval::new)
                        .collect(Collectors.toList());
            }
            this.alignmentIntervals = Collections.unmodifiableList(intervals);
            this.contigSequence = bases;
            this.contigName = read.getName();
        }
    }

    /**
     * Composes a {@link AlignedContig} from all its constituted reads.
     * @param reads {@link Iterable} containing all relevant reads concerning the aligned-contig to compose.
     * @return never {@code null}.
     * @throws IllegalArgumentException if {@code reads} is {@code null} or contains an inconsistent or incomplete
     *    set or read records.
     */
    public static AlignedContig of(final Iterable<GATKRead> reads) {
        Utils.nonNull(reads);
        return of(reads.iterator());
    }

    /**
     * Composes a {@link AlignedContig} from all its constituted reads.
     * @param reads {@link Stream} containing all the relevant read record for this contig.
     * @return never {@code null}.
     * @throws IllegalArgumentException if {@code reads} is {@code null} or contains an inconsistent or incomplete
     *    set or read records.
     */
    public static AlignedContig of(final Stream<GATKRead> reads) {
        Utils.nonNull(reads);
        return of(reads.iterator());
    }

    /**
     * Composes a {@link AlignedContig} from all its constituted reads.
     * @param reads iterator that contains the reads as the remaining records to iterate upon.
     * @return never {@code null}.
     * @throws IllegalArgumentException if {@code reads} is {@code null} or contains an inconsistent or incomplete
     *    set or read records.
     */
    public static AlignedContig of(final Iterator<GATKRead> reads) {
        Utils.nonNull(reads);
        if (!reads.hasNext()) {
            throw new IllegalArgumentException("there must be at least one read");
        } else {
            final GATKRead first = reads.next();
            if (!reads.hasNext()) {
                return new AlignedContig(first);
            } else {
                final String name = first.getName();
                final List<AlignmentInterval> alignmentIntervals = new ArrayList<>(5);
                final int length;
                final byte[] bases;
                if (first.isUnmapped()) {
                    length = first.getLength();
                    bases = Utils.nonNull(first.getBases(), "an unmapped record must have bases");
                    if (length != bases.length) {
                        throw new IllegalArgumentException("mismatch between bases array length and declared read length in unmapped record");
                    }
                } else {
                    final Cigar cigar = first.getCigar();
                    if (cigar == null || cigar.isEmpty()) {
                        throw new IllegalArgumentException("a mapped read must have a non-null nor empty cigar");
                    } else {
                        length = CigarUtils.countUnclippedReadBases(cigar);
                    }
                    bases = new byte[length];
                }
                GATKRead next = first;
                int basesDefined = 0;
                do {
                    if (next.isPaired()) {
                        throw new IllegalArgumentException("onlyt paired reads are currently supported");
                    } else if (!next.getName().equals(name)) {
                        throw new IllegalArgumentException("the input contains a mix of different reads");
                    } else {
                        basesDefined = mergeBases(basesDefined, bases, next);
                    }
                    if (!next.isUnmapped()) {
                        alignmentIntervals.add(new AlignmentInterval(next));
                    }
                    next = reads.hasNext() ? reads.next() : null;
                } while (next != null);
                if (basesDefined < length) {
                    throw new IllegalArgumentException("missing bases when looking across all the record provided: " + name + " " + basesDefined + " " + length + " " + AlignmentInterval.encode(alignmentIntervals));
                }
                return new AlignedContig(name, bases, alignmentIntervals);
            }
        }
    }

    private static int mergeBases(final int startBasesDefined, final byte[] bases, final GATKRead next) {
        if (startBasesDefined == bases.length) { // we have values for all the bases already we skip this step.
            return startBasesDefined;
        } else {
            final byte[] nextBases = Utils.nonNull(next.getBases(), "bases must be defined for every record");
            final int from, to;
            if (next.isUnmapped()) {
                if (nextBases.length != bases.length) {
                    throw new IllegalArgumentException("the input read bases for an unmapped read must be as long as the expected");
                }
                from = 0; to = bases.length;
            } else {
                final Cigar readCigar = next.getCigar();
                final Cigar cigar = next.isReverseStrand() ? CigarUtils.invertCigar(readCigar) : readCigar;
                if (cigar == null || cigar.isEmpty()) {
                    throw new IllegalArgumentException("mapped records must have a non-empty cigar");
                }
                from = CigarUtils.countLeftHardClippedBases(cigar);
                to = bases.length - CigarUtils.countRightHardClippedBases(cigar);
                if (next.isReverseStrand()) {
                    SequenceUtil.reverseComplement(nextBases);
                }
            }
            return startBasesDefined + mergeBases(nextBases, bases, from, to);
        }
    }

    private static int mergeBases(final byte[] src, final byte[] target, final int from, final int to) {
        int newlyDefined = 0;
        for (int i = from, j = 0; i < to; i++, j++) {
            final byte s = src[j], t = target[i];
            if (s == 0) {
                throw new IllegalArgumentException("a record contain null bases");
            } else if (t == 0) {
                target[i] = s;
                newlyDefined++;
            } else if (s != t) {
                throw new IllegalArgumentException("a record contains mismatching bases");
            }
        }
        return newlyDefined;
    }

    public AlignedContig(final SVHaplotype haplotype, final String name) {
        Utils.nonNull(haplotype);
        Utils.nonNull(name);
        this.contigName = name;
        this.contigSequence = haplotype.getBases();
        this.alignmentIntervals = haplotype.getReferenceAlignmentIntervals();
    }

    public AlignedContig(final String contigName, final byte[] contigSequence, final List<AlignmentInterval> alignmentIntervals) {
        this.contigName = contigName;
        this.contigSequence = contigSequence;
        this.alignmentIntervals = alignmentIntervals.stream().sorted(getAlignmentIntervalComparator()).collect(Collectors.toList());
    }

    public AlignedContig(final Kryo kryo, final Input input) {

        contigName = input.readString();

        final int nBases = input.readInt();
        contigSequence = new byte[nBases];
        for (int b = 0; b < nBases; ++b) {
            contigSequence[b] = input.readByte();
        }

        final int nAlignments = input.readInt();
        alignmentIntervals = new ArrayList<>(nAlignments);
        for (int i = 0; i < nAlignments; ++i) {
            alignmentIntervals.add(new AlignmentInterval(kryo, input));
        }
    }

    static Comparator<AlignmentInterval> getAlignmentIntervalComparator() {
        Comparator<AlignmentInterval> comparePos = Comparator.comparingInt(aln -> aln.startInAssembledContig);
        Comparator<AlignmentInterval> compareRefTig = Comparator.comparing(aln -> aln.referenceSpan.getContig());
        Comparator<AlignmentInterval> compareRefSpanStart = Comparator.comparingInt(aln -> aln.referenceSpan.getStart());
        return comparePos.thenComparing(compareRefTig).thenComparing(compareRefSpanStart);
    }

    boolean hasOnly2Alignments() {
        return alignmentIntervals.size() == 2;
    }

    /**
     * @return first alignment of the contig
     */
    public AlignmentInterval getHeadAlignment() {
        return alignmentIntervals.get(0);
    }

    /**
     * @return last alignment of the contig
     */
    public AlignmentInterval getTailAlignment() {
        return alignmentIntervals.get(alignmentIntervals.size() - 1);
    }

    public String getContigName() {
        return contigName;
    }

    public byte[] getContigSequence() {
        return contigSequence;
    }

    public boolean isUnmapped() {
        return alignmentIntervals.isEmpty();
    }

    public List<AlignmentInterval> getAlignments() {
        return alignmentIntervals;
    }

    @Override
    public String toString() {
        return formatContigInfo(
                new Tuple2<>(contigName, alignmentIntervals.stream().map(AlignmentInterval::toPackedString).collect(Collectors.toList())));
    }

    public static void toFasta(final File file, final ReferenceSource reference, final AlignedContig contig, final int padding, final boolean matchedBasedAsDots) throws IOException {
        try (final Writer writer = new FileWriter(file)){
            toFasta(writer, reference, contig, padding, matchedBasedAsDots);
        } catch (final IOException ex) {
            throw new UserException.CouldNotCreateOutputFile(file, ex.getMessage(), ex);
        }
    }

    public static void toFasta(final Writer writer, final ReferenceSource reference, final AlignedContig contig, final int padding, final boolean matchedBasesAsDots) throws IOException {
        writer.append(toFastaString(reference, contig, padding, matchedBasesAsDots));
    }

    public static String toFastaString(final ReferenceSource reference, final AlignedContig contig, final int padding, final boolean matchedBasesAsDots)
            throws IOException
    {
        Utils.nonNull(reference);
        Utils.nonNull(contig);
        final SAMSequenceDictionary dictionary = Utils.nonNull(reference.getReferenceSequenceDictionary());
        final List<SimpleInterval> refereneSpans = contig.alignmentIntervals.stream()
                .map(ai -> {
                    final SimpleInterval referenceSpan = ai.referenceSpan;
                    final int leftSoftClip = ai.cigarAlongReference().getCigarElement(0).getOperator() == CigarOperator.S ?
                            ai.cigarAlongReference().getCigarElement(0).getLength() : 0;
                    final int rightSoftClip = ai.cigarAlongReference().getLastCigarElement().getOperator() == CigarOperator.S ?
                            ai.cigarAlongReference().getLastCigarElement().getLength() : 0;
                    if (leftSoftClip == 0 && rightSoftClip == 0 && padding == 0) {
                        return referenceSpan;
                    } else {
                        return new SimpleInterval(referenceSpan.getContig(), referenceSpan.getStart() - leftSoftClip - padding, referenceSpan.getEnd() + rightSoftClip + padding);
                    }
                }).collect(Collectors.toList());
        Collections.sort(refereneSpans, IntervalUtils.getDictionaryOrderComparator(reference.getReferenceSequenceDictionary()));
        final Deque<SimpleInterval> mergedSpans = new ArrayDeque<>(refereneSpans.size());
        SimpleInterval last;
        mergedSpans.add(last = refereneSpans.get(0));
        for (int i = 1; i < refereneSpans.size(); i++) {
            final SimpleInterval next = refereneSpans.get(i);
            if (last.overlapsWithMargin(next, 1)) {
                mergedSpans.removeLast();
                mergedSpans.addLast(last = last.mergeWithContiguous(next));
            } else {
                mergedSpans.addLast(last = next);
            }
        }
        final StringBuilder output = new StringBuilder(10000);
        for (final SimpleInterval referenceSpan : mergedSpans) {
            final Stream<ImmutablePair<Integer, Integer>> insertions = contig.getAlignments().stream()
                    .filter(ai -> referenceSpan.contains(ai.referenceSpan))
                    .filter(ai -> ai.cigarAlong5to3DirectionOfContig.containsOperator(CigarOperator.I))
                    .flatMap(ai -> {
                        final List<ImmutablePair<Integer, Integer>> insertionAndLengths = new ArrayList<>(ai.cigarAlong5to3DirectionOfContig.numCigarElements());
                        int referencePostion = ai.referenceSpan.getStart();
                        for (final CigarElement e : ai.cigarAlongReference()) {
                            if (e.getOperator().consumesReferenceBases()) {
                                referencePostion += e.getLength();
                            } else if (e.getOperator() == CigarOperator.I) {
                                insertionAndLengths.add(new ImmutablePair<>(referencePostion, e.getLength()));
                            }
                        }
                        return insertionAndLengths.stream();
                    });
            final Map<Integer, Integer> insertionPositionAndMaxLength = insertions
                    //           .collect(Collectors.groupingBy(p -> p.getLeft()));
                    .collect(Collectors.groupingBy(ImmutablePair::getLeft, Collectors.mapping(ImmutablePair::getRight, Collectors.reducing(0, Math::max))));
            final int width = referenceSpan.size() + insertionPositionAndMaxLength.values().stream().mapToInt(i -> i).sum();
            Utils.nonNull(referenceSpan.getContig());
            final int beyondContigEndPadding = referenceSpan.getEnd() - dictionary.getSequence(referenceSpan.getContig()).getSequenceLength();
            final SimpleInterval actualReferenceSpan;
            output.append('>');
            if (beyondContigEndPadding <= 0) {
                output.append(referenceSpan);
                actualReferenceSpan = referenceSpan;
            } else {
                actualReferenceSpan = new SimpleInterval(referenceSpan.getContig(), referenceSpan.getStart(), dictionary.getSequence(referenceSpan.getContig()).getSequenceLength());
                output.append(actualReferenceSpan);
                output.append("\t(").append(beyondContigEndPadding).append(" gap padded beyond end");
            }
            output.append('\n');
            final int refSeqOffset = output.length();
            final byte[] refBases = reference.getReferenceBases(actualReferenceSpan).getBases();
            for (int i = 0, pos = referenceSpan.getStart(); pos <= actualReferenceSpan.getEnd(); ++i, ++pos) {
                final int insertLength = insertionPositionAndMaxLength.getOrDefault(pos, 0);
                if (insertLength > 0) {
                    for (int j = 0; j < insertLength; ++j) {
                        output.append('-');
                    }
                }
                output.append(Character.toUpperCase((char) refBases[i]));
            }
            for (int i = 0; i < beyondContigEndPadding; i++) {
                output.append('-');
            }
            final byte[] referenceBasesWithGaps = output.subSequence(refSeqOffset, output.length()).toString().getBytes();
            output.append('\n');

            contig.getAlignments().stream()
                    .filter(ai -> actualReferenceSpan.contains(ai.referenceSpan.expandWithinContig(padding, dictionary)))
                    .forEach(ai -> {
                        output.append('>').append(contig.contigName).append(':').append(ai.startInAssembledContig).append('-').append(ai.endInAssembledContig);
                        output.append('\t');
                        ai.appendSATagString(output);
                        output.append('\n');
                        final List<CigarElement> cigar = new ArrayList<>(ai.cigarAlong5to3DirectionOfContig.numCigarElements() + 2);

                        // Since we need to skip to the first aligned reference position we artificailly add a deletion operation
                        // that would consume those references bases before the first aligned based to the contig:
                        if (actualReferenceSpan.getStart() < ai.referenceSpan.getStart()) {
                            cigar.add(new CigarElement(ai.referenceSpan.getStart() - actualReferenceSpan.getStart(), CigarOperator.D));
                        }
                        cigar.addAll(ai.cigarAlongReference().getCigarElements());
                        int linePos = 0;
                        final int contigBaseIncrease = ai.forwardStrand ? 1 : -1; // we move forward or backward in the contig sequence?
                        int nextContigBase = (ai.forwardStrand ? ai.startInAssembledContig - CigarUtils.countLeftClippedBases(ai.cigarAlong5to3DirectionOfContig)
                                : ai.endInAssembledContig + CigarUtils.countRightSoftClippedBases(ai.cigarAlong5to3DirectionOfContig)) - 1;
                        for (final CigarElement e : cigar) {
                            final int length = e.getLength();
                            final CigarOperator o = e.getOperator();
                            if (o.consumesReferenceBases() && !o.consumesReadBases()) {
                                for (int remaining = length; remaining > 0; ++linePos) {
                                    if (referenceBasesWithGaps[linePos] != '-') {
                                        remaining--;
                                    } else {
                                        System.err.append('.');
                                    }
                                    output.append('-');
                                }
                            } else if (o == CigarOperator.S) {
                                if (output.length() == 0 || output.charAt(output.length() - 1) == '-') {
                                    for (; referenceBasesWithGaps[linePos] == '-'; ++linePos) {
                                        output.append('-');
                                    } // skip to the next ref base.
                                    output.setLength(output.length() - length);
                                    linePos -= length;
                                }
                                for (int i = 0; i < length; i++, ++linePos) {
                                    final byte contigBase = ai.forwardStrand
                                            ? contig.contigSequence[nextContigBase]
                                            : Nucleotide.complement(contig.contigSequence[nextContigBase]);
                                    output.append(Character.toLowerCase((char) contigBase));
                                    nextContigBase += contigBaseIncrease;
                                }
                            } else if (o.consumesReadBases() && o.consumesReferenceBases()) {
                                for (int remaining = length; remaining > 0; ++linePos) {
                                    if (referenceBasesWithGaps[linePos] != '-') {
                                        remaining--;
                                        final byte contigBase = ai.forwardStrand
                                                ? contig.contigSequence[nextContigBase]
                                                : Nucleotide.complement(contig.contigSequence[nextContigBase]);
                                        if (matchedBasesAsDots && Nucleotide.same(referenceBasesWithGaps[linePos], contigBase)) {
                                            output.append('.');
                                        } else {
                                            output.append(Character.toUpperCase((char) contigBase));
                                        }
                                        nextContigBase += contigBaseIncrease;
                                    } else {
                                        output.append('-');
                                    }
                                }
                            } else if (o.consumesReadBases()) {// && !o.consumesReferenceBases())
                                for (int i = 0; i < length; i++, ++linePos) {
                                    final byte contigBase = ai.forwardStrand
                                            ? contig.contigSequence[nextContigBase]
                                            : Nucleotide.complement(contig.contigSequence[nextContigBase]);
                                    output.append(Character.toUpperCase((char) contigBase));
                                    nextContigBase += contigBaseIncrease;
                                }
                                while (referenceBasesWithGaps[linePos] == '-') {
                                    output.append('-');
                                    linePos++;
                                }
                            }
                        }
                        for (; linePos < width; ++linePos) {
                            output.append('-');
                        }
                        output.append('\n');
                    });
            while (Character.isWhitespace(output.charAt(output.length() - 1))) {
                output.setLength(output.length() - 1);
            }
        }
        return output.toString();
    }

    /**
     * Format provided {@code tigNameAndMappings} for debugging.
     */
    public static String formatContigInfo(final Tuple2<String, List<String>> tigNameAndMappings) {
        return "(" + tigNameAndMappings._1 + ", " + tigNameAndMappings._2 + ")";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final AlignedContig that = (AlignedContig) o;

        if (!contigName.equals(that.contigName)) return false;
        if (!Arrays.equals(contigSequence, that.contigSequence)) return false;
        return alignmentIntervals.equals(that.alignmentIntervals);
    }

    @Override
    public int hashCode() {
        int result = contigName.hashCode();
        result = 31 * result + Arrays.hashCode(contigSequence);
        result = 31 * result + alignmentIntervals.hashCode();
        return result;
    }

    void serialize(final Kryo kryo, final Output output) {

        output.writeString(contigName);

        output.writeInt(contigSequence.length);
        for (final byte base : contigSequence) {
            output.writeByte(base);
        }

        output.writeInt(alignmentIntervals.size());
        alignmentIntervals.forEach(it -> it.serialize(kryo, output));

    }

    public static final class Serializer extends com.esotericsoftware.kryo.Serializer<AlignedContig> {
        @Override
        public void write(final Kryo kryo, final Output output, final AlignedContig alignedContig) {
            alignedContig.serialize(kryo, output);
        }

        @Override
        public AlignedContig read(final Kryo kryo, final Input input, final Class<AlignedContig> clazz) {
            return new AlignedContig(kryo, input);
        }
    }
}
