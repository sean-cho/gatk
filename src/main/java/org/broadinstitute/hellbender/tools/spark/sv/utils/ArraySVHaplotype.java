package org.broadinstitute.hellbender.tools.spark.sv.utils;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.alignment.AlignmentInterval;
import org.broadinstitute.hellbender.utils.Nucleotide;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by valentin on 10/11/17.
 */
@DefaultSerializer(ArraySVHaplotype.Serializer.class)
public class ArraySVHaplotype extends AbstractSVHaplotype {

    protected final byte[] bases;
    protected final boolean isContig;

    public ArraySVHaplotype(final String name, final List<AlignmentInterval> intervals, final byte[] bases, final String variantId, final SimpleInterval variantLocation, final boolean isContig) {
        super(name, intervals, variantId, variantLocation);
        this.bases = bases;
        for (int i = 0; i < bases.length; i++) {
            if (Nucleotide.decode(bases[i]) == Nucleotide.INVALID) {
                throw new IllegalArgumentException("invalid base at " + i + " " + bases[i]);
            }
        }
        this.isContig = isContig;
    }

    public static ArraySVHaplotype of(final GATKRead read) {
        final byte[] bases = read.getBases();
        final boolean isContig = "CTG".equals(read.getReadGroup());
        final String variantId = read.getAttributeAsString("VC");
        final List<AlignmentInterval> saIntervals = read.hasAttribute("SA") ? AlignmentInterval.decodeList(read.getAttributeAsString("SA")) : Collections.emptyList();
        final List<AlignmentInterval> allIntervals;
        if (read.isUnmapped()) {
            allIntervals = saIntervals;
        } else {
            allIntervals = new ArrayList<>();
            allIntervals.add(new AlignmentInterval(read));
            allIntervals.addAll(saIntervals);
        }
        return new ArraySVHaplotype(read.getName(), allIntervals, bases, variantId,
                new SimpleInterval(read.getAssignedContig(), read.getAssignedStart()), isContig);
    }

    protected ArraySVHaplotype(final Kryo kryo, final Input input) {
        super(kryo, input);
        final int length = input.readInt();
        this.bases = input.readBytes(length);
        this.isContig = input.readBoolean();
    }

    @Override
    public int getLength() {
        return bases.length;
    }

    @Override
    public boolean isContig() {
        return isContig;
    }

    protected void unsafeCopyBases(final int offset, final byte[] dest, final int destOffset, final int length) {
        System.arraycopy(bases, offset, dest, destOffset, length);
    }

    public <T> List<List<AlignmentInterval>> align(final Iterable<T> input, Function<T, byte[]> basesOf) {
        try (final SingleContigReferenceAligner aligner = new SingleContigReferenceAligner(name, bases)) {
            final List<byte[]> seqs = Utils.stream(input).map(basesOf).collect(Collectors.toList());
            return aligner.align(seqs);
        } catch (final RuntimeException ex) {
            throw new GATKException("could not create aligner", ex);
        }
    }

    public static class Serializer<S extends ArraySVHaplotype> extends AbstractSVHaplotype.Serializer<S> {

        @Override
        public void write(final Kryo kryo, final Output output, final S object) {
            super.write(kryo, output, object);
            output.writeInt(object.bases.length);
            output.write(object.bases);
            output.writeBoolean(object.isContig);
        }

        @Override
        @SuppressWarnings("unchecked")
        public S read(Kryo kryo, Input input, Class<S> type) {
            if (type != ArraySVHaplotype.class) {
                throw new IllegalArgumentException("the input class must be: " + ArraySVHaplotype.class);
            } else {
                return (S) new ArraySVHaplotype(kryo, input);
            }
        }
    }

}
