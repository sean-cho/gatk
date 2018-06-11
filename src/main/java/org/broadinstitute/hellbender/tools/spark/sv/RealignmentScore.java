package org.broadinstitute.hellbender.tools.spark.sv;

import htsjdk.samtools.CigarElement;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.alignment.AlignmentInterval;
import org.broadinstitute.hellbender.utils.Nucleotide;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.param.ParamUtils;
import org.broadinstitute.hellbender.utils.read.CigarUtils;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class to represent and calculate the aligned contig score.
 */
public final class RealignmentScore {

    public final double matchPenalty;
    public final double mismatchPenalty;
    public final double gapOpenPenalty;
    public final double gapExtendPenalty;

    public final int numberOfIndels;
    public final int numberOfMatches;
    public final int numberOfMismatches;
    public final int indelLengthSum;

    private final double value;

    public RealignmentScore(final RealignmentScoreArgumentCollection parameters, final int matches, final int mismatches, final int indels, final int totalIndelLength) {
        ParamUtils.isPositiveOrZero(matches, "number of matches cannot be negative");
        ParamUtils.isPositiveOrZero(mismatches, "number of mismatches cannot be negative");
        ParamUtils.isPositiveOrZero(indels, "number of indels cannot be negative");
        ParamUtils.isPositiveOrZero(totalIndelLength - indels, "total length of indels minus the number of indels cannot be negative");
        this.gapExtendPenalty = parameters.gapExtendPenalty;
        this.gapOpenPenalty = parameters.gapOpenPenalty;
        this.mismatchPenalty = parameters.mismatchPenalty;
        this.matchPenalty = parameters.matchPenalty;
        this.numberOfIndels = indels;
        this.numberOfMatches = matches;
        this.numberOfMismatches = mismatches;
        this.indelLengthSum = totalIndelLength;
        this.value = numberOfIndels * gapOpenPenalty
                + numberOfMatches * matchPenalty
                + numberOfMismatches * mismatchPenalty
                + (indelLengthSum - numberOfIndels) * gapExtendPenalty;

    }

    public static RealignmentScore valueOf(final String str, final RealignmentScoreArgumentCollection parameters) {
        final String[] parts = Utils.nonNull(str).split("[:,]");
        Utils.validateArg(parts.length == 5, "the input string has the wrong number of components");
        int nextIdx = 0;
        // we only check that the first value is a valid double (score), we don't keep this value as is derivable from
        // the other based on penalties.
        ParamUtils.isDouble(parts[nextIdx++], "score is not a valid double value");
        final int matches = ParamUtils.isPositiveOrZeroInteger(parts[nextIdx++], "matches is not a valid positive integer");
        final int misMatches = ParamUtils.isPositiveOrZeroInteger(parts[nextIdx++], "misMatches is not a valid positive integer");
        final int indels = ParamUtils.isPositiveOrZeroInteger(parts[nextIdx++], "indels is not a valid positive integer");
        final int indelLenghts = ParamUtils.isPositiveOrZeroInteger(parts[nextIdx++], "indel-length is not a valid positive integer");
        return new RealignmentScore(parameters, matches, misMatches, indels, indelLenghts);
    }

    private static RealignmentScore calculate(final RealignmentScoreArgumentCollection parameters,
                                              final int direction, final byte[] ref, final byte[] seq,
                                              final List<AlignmentInterval> intervals) {
        int totalIndels = 0;
        int totalMatches = 0;
        int totalMismatches = 0;
        int totalIndelLength = 0;
        for (int i = 0; i < intervals.size(); i++) {
            final AlignmentInterval ai = intervals.get(i);
            if (i > 0) {
                final AlignmentInterval prev = intervals.get(i - 1);

                final AlignmentInterval left = direction > 0 ? prev : ai; // left on the reference
                final AlignmentInterval right = direction > 0 ? ai : prev; // right on the reference
                final int refIndelLength = right.referenceSpan.getStart() - left.referenceSpan.getEnd() - 1;
                final int ctgIndelLength = prev.endInAssembledContig - ai.startInAssembledContig - 1;
                if (refIndelLength != 0 || ctgIndelLength != 0) {
                    totalIndels++;
                    final int indelLength = Math.max(Math.abs(refIndelLength) + Math.max(0, -ctgIndelLength), Math.abs(ctgIndelLength));
                    // The max(0, -ctgIndelLength) is to correct of short overlaps on the contig due to short "unclipped" soft-clips.
                    totalIndelLength += indelLength;
                }
            }
            final int totalAligned = ai.cigarAlong5to3DirectionOfContig.getCigarElements().stream()
                    .filter(ce -> ce.getOperator().isAlignment())
                    .mapToInt(CigarElement::getLength).sum();
            final int misMatches = calculateMismatches(ref, seq, ai);
            final int matches = totalAligned - misMatches;
            final int indelCount = (int) ai.cigarAlong5to3DirectionOfContig.getCigarElements().stream()
                    .filter(ce -> ce.getOperator().isIndel())
                    .count();
            final int indelLengthSum = ai.cigarAlong5to3DirectionOfContig.getCigarElements().stream()
                    .filter(ce -> ce.getOperator().isIndel())
                    .mapToInt(CigarElement::getLength).sum();
            totalIndels += indelCount;
            totalMatches += matches;
            totalMismatches += misMatches;
            totalIndelLength += indelLengthSum;
        }
        if (intervals.isEmpty()) {
            totalIndelLength += seq.length;
            totalIndels++;
        } else {
            if (intervals.get(0).startInAssembledContig > 1) {
                final int indelLength =  Math.min(intervals.get(0).startInAssembledContig - 1, intervals.get(0).referenceSpan.getStart());
                totalIndelLength += indelLength;
                totalIndels++;
            }
            if (intervals.get(intervals.size() - 1).endInAssembledContig < seq.length) {
                final int indelLength = seq.length - intervals.get(intervals.size() - 1).endInAssembledContig;
                totalIndelLength += indelLength;
                totalIndels++;
            }
        }
        return new RealignmentScore(parameters, totalMatches, totalMismatches, totalIndels, totalIndelLength);

    }

    public static RealignmentScore calculate(final RealignmentScoreArgumentCollection parameters, final byte[] ref, final byte[] seq, final List<AlignmentInterval> alignmentIntervals) {
        final List<AlignmentInterval> intervals = alignmentIntervals.stream()
                .sorted(Comparator.comparing(ai -> ai.startInAssembledContig))
                .collect(Collectors.toList());

        final int forwardAlignedBases = intervals.stream().filter(ai -> ai.forwardStrand && ai.mapQual >= parameters.minimumMappingQuality).mapToInt(ai -> CigarUtils.countAlignedBases(ai.cigarAlong5to3DirectionOfContig)).sum();
        final int reverseAlignedBases = intervals.stream().filter(ai -> !ai.forwardStrand && ai.mapQual >= parameters.minimumMappingQuality).mapToInt(ai -> CigarUtils.countAlignedBases(ai.cigarAlong5to3DirectionOfContig)).sum();
        if (forwardAlignedBases >= reverseAlignedBases) {
            return calculate(parameters, 1, ref, seq, alignmentIntervals.stream().filter(ai -> ai.forwardStrand).collect(Collectors.toList()));
        } else {
            return calculate(parameters, -1, ref, seq, alignmentIntervals.stream().filter(ai -> !ai.forwardStrand).collect(Collectors.toList()));
        }
    }

    private static int calculateMismatches(final byte[] ref, final byte[] seq, final AlignmentInterval ai) {
        int refOffset = ai.referenceSpan.getStart() - 1;
        int direction = ai.forwardStrand ? 1 : -1;
//        int seqOffset = direction == 1 ? (ai.startInAssembledContig - 1 - CigarUtils.countLeftHardClippedBases(ai.cigarAlong5to3DirectionOfContig))
//                : (ai.endInAssembledContig - 1 - CigarUtils.countRightHardClippedBases(ai.cigarAlong5to3DirectionOfContig));
        int seqOffset = direction == 1 ? (ai.startInAssembledContig - 1) : (ai.endInAssembledContig - 1);
        final List<CigarElement> elements = ai.cigarAlongReference().getCigarElements();
        int index = 0;
        while (index < elements.size() && elements.get(index).getOperator().isClipping()) {
            index++;
        }
        int result = 0;
        while (index < elements.size() && !elements.get(index).getOperator().isClipping()) {
            final CigarElement element = elements.get(index++);
            if (element.getOperator().isAlignment()) {
                for (int i = 0, j = 0; i < element.getLength(); i++, j += direction) {
                    if (ai.forwardStrand && !Nucleotide.decode(ref[refOffset + i]).intersects(Nucleotide.decode(seq[seqOffset + j]))) {
                        result++;
                    } else if (!ai.forwardStrand && !Nucleotide.decode(ref[refOffset + i]).complement().intersects(Nucleotide.decode(seq[seqOffset + j]))) {
                        result++;
                    }
                }
            }
            if (element.getOperator().consumesReferenceBases()) refOffset += element.getLength();
            if (element.getOperator().consumesReadBases()) seqOffset += element.getLength() * direction;
        }
        return result;
    }

    public String toString() {
        return getPhredValue() + ":" + Utils.join(",", numberOfMatches, numberOfMismatches,
                numberOfIndels, indelLengthSum);
    }

    public double getPhredValue() {
        return value;
    }

    public double getLog10Prob() {
        return value * -.1;
    }
}