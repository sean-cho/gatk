package org.broadinstitute.hellbender.tools.spark.sv;

import htsjdk.samtools.Cigar;
import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.alignment.AlignmentInterval;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import shapeless.ops.coproduct;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by valentin on 6/9/18.
 */
public class AlignmentProjector {
//
//    private final List<AlignmentInterval> map;
//
//    public AlignmentProjector(final int sequenceLength, final List<AlignmentInterval> map) {
//        this.map = disolveIndels(map);
//    }
//
//    private static List<AlignmentInterval> disolveIndels(final List<AlignmentInterval> map) {
//        map.stream()
//                .flatMap(ai -> {
//                    final Cigar cigar = ai.cigarAlong5to3DirectionOfContig;
//                    if (cigar.numCigarElements() == 1 && cigar.getCigarElement(0).getOperator().isAlignment()) {
//                        return Collections.singletonList(ai).iterator();
//                    } else {
//                        final List<AlignmentInterval> result = new ArrayList<>(cigar.numCigarElements());
//                        int nextSeqIndex = ai.startInAssembledContig;
//                        int direction = ai.forwardStrand ? 1 : -1;
//                        int nextRefIndex = ai.forwardStrand ? ai.referenceSpan.getStart() : ai.referenceSpan.getEnd();
//                        for (final CigarElement ce : cigar) {
//                            final CigarOperator op = ce.getOperator();
//                            final int length = ce.getLength();
//                            if (op.isAlignment()) {
//                                final int startOnAssembledContig = nextSeqIndex;
//                                final int endOnAssembledContig = nextSeqIndex + length - 1;
//                                final int startOnReference = ai.forwardStrand ? nextRefIndex : nextRefIndex + length - 1;
//                                final int endOnReference = ai.forwardStrand ? nextRefIndex + length - 1 : nextRefIndex;
//                                final SimpleInterval referenceSpan = new SimpleInterval(ai.referenceSpan.getContig(), nextRefIndex)
//                            }
//                        }
//                    }
//
//                });
//    }
}
