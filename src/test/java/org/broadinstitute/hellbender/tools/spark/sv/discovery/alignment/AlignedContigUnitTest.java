package org.broadinstitute.hellbender.tools.spark.sv.discovery.alignment;

import htsjdk.samtools.Cigar;
import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.broadinstitute.hellbender.GATKBaseTest;
import org.broadinstitute.hellbender.engine.datasources.ReferenceMultiSource;
import org.broadinstitute.hellbender.engine.datasources.ReferenceSource;
import org.broadinstitute.hellbender.engine.datasources.ReferenceWindowFunctions;
import org.broadinstitute.hellbender.utils.Nucleotide;
import org.broadinstitute.hellbender.utils.RandomDNA;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.reference.FastaReferenceWriterUnitTest;
import org.broadinstitute.hellbender.utils.reference.ReferenceBases;
import org.broadinstitute.hellbender.utils.test.ReadTestUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by valentin on 4/23/18.
 */
public class AlignedContigUnitTest extends GATKBaseTest {

    private static String twoBitRefURL = publicTestDir + "large/human_g1k_v37.20.21.2bit";

    private static final ReferenceSource REFERENCE = new ReferenceMultiSource(twoBitRefURL, ReferenceWindowFunctions.IDENTITY_FUNCTION);


    @Test(dataProvider = "toFastaData")
    public void testToFasta(final AlignedContig contig, final int padding, final boolean matchesAsDot, final String expected)
        throws IOException {
        final String actual = AlignedContig.toFastaString(REFERENCE, contig, padding, matchesAsDot);
//        if (!actual.equals(expected)) {
//            Assert.assertEquals(actual.length(), expected.length());
//            for (int i = 0; i < actual.length(); i++) {
//                Assert.assertEquals(actual.charAt(i), expected.charAt(i), "Character at " + (i+1) + "/" + (actual.length()) +  " is different " + actual.charAt(i) + " != " + expected.charAt(i));
 //           }
 //       }
        Assert.assertEquals(actual, expected);
    }

    @DataProvider(name="toFastaData")
    public Object[][] toFastaData() throws IOException {
        int i = 0;
        int j = 0;
        try {
            final Random rdn = new Random(13);
            final RandomDNA randomDNA = new RandomDNA(rdn);

            final List<Object[]> result = new ArrayList<>();
            final SAMSequenceRecord chr1 = REFERENCE.getReferenceSequenceDictionary().getSequence(0);
            final SAMSequenceRecord chr2 = REFERENCE.getReferenceSequenceDictionary().getSequence(1);

            final ReferenceBases chr1_101000_102000 = REFERENCE.getReferenceBases(new SimpleInterval(chr1.getSequenceName(), 101000, 102000));
            final ReferenceBases chr1_101000_102000_10padding = REFERENCE.getReferenceBases(new SimpleInterval(chr1.getSequenceName(), 101000 - 10, 102000 + 10));
            final AlignedContig chr1_101000_102000_pertch_match = new AlignedContig("ctg001", chr1_101000_102000.getBases(), Collections.singletonList(new AlignmentInterval(chr1.getSequenceName() + ",101000,+,1001M,40,0,40")));
            result.add(new Object[]{chr1_101000_102000_pertch_match, 0, false,
                    chr1_101000_102000.appendBasesTo(new StringBuilder(10000).append('>').append(chr1.getSequenceName()).append(":101000-102000\n"), chr1_101000_102000.getInterval().getStart(), chr1_101000_102000.getInterval().getEnd())
                            .append("\n>ctg001:1-1001\t").append(chr1.getSequenceName()).append(",101000,+,1001M,40,0,40\n").append(chr1_101000_102000.toBaseString()).toString()});

            result.add(new Object[]{chr1_101000_102000_pertch_match, 10, true,
                    '>' + chr1.getSequenceName() + ":100990-102010\n" + chr1_101000_102000_10padding.toBaseString() + '\n' +
                            ">ctg001:1-1001\t" + chr1.getSequenceName() + ",101000,+,1001M,40,0,40\n" + StringUtils.repeat("-", 10) + StringUtils.repeat(".", 1001) + StringUtils.repeat("-", 10)});

            final byte[] originalBases = chr1_101000_102000.getBases().clone();
            byte[] referenceBases = chr1_101000_102000_10padding.getBases().clone();
            byte[] mutatedBases = originalBases.clone();
            final List<CigarElement> cigar = new ArrayList<>();
            double mutationRate = 0.01;
            char[] dotsMutatedBases = new char[mutatedBases.length];
            double indelRate = 0.005;
            double extensionRate = 0.75;
            int matchStart = -1;
            int r;
            for (i = 0, j = 0, r = 10; i < originalBases.length; i++, r++) {
                if (rdn.nextDouble() < indelRate) {
                    if (matchStart >= 0) cigar.add(new CigarElement(j - matchStart, CigarOperator.M));
                    matchStart = -1;
                    final boolean deletion = rdn.nextBoolean();
                    int length = 1;
                    while (j + length < mutatedBases.length && rdn.nextDouble() < extensionRate) {
                        length++;
                    }
                    if (deletion) {
                        Arrays.fill(mutatedBases, j, j + length, (byte) '-');
                        Arrays.fill(dotsMutatedBases, j, j + length, '-');
                        cigar.add(new CigarElement(length, CigarOperator.D));
                        i += length - 1;
                        r += length - 1;
                    } else {
                        final byte[] insert = randomDNA.nextBases(length);
                        mutatedBases = Arrays.copyOf(mutatedBases, mutatedBases.length + length);
                        System.arraycopy(mutatedBases, j, mutatedBases, j + length, mutatedBases.length - length - j);
                        dotsMutatedBases = Arrays.copyOf(dotsMutatedBases, dotsMutatedBases.length + length);
                        System.arraycopy(dotsMutatedBases, j, dotsMutatedBases, j + length, dotsMutatedBases.length - length - j);
                        System.arraycopy(insert, 0, mutatedBases, j, length);
                        for (int k = 0; k < length; k++) {
                            dotsMutatedBases[j + k] = (char) insert[k];
                        }
                        referenceBases = Arrays.copyOf(referenceBases, referenceBases.length + length);
                        System.arraycopy(referenceBases, r, referenceBases, r + length, referenceBases.length - r - length);
                        Arrays.fill(referenceBases, r, r + length, (byte) '-');
                        cigar.add(new CigarElement(length, CigarOperator.I));
                        r += length - 1;
                    }
                    j += length;
                    indelRate = 0; // make sure we don't have consecutive indels.
                } else if (rdn.nextDouble() < mutationRate) {
                    indelRate = 0.01; // reset indel-rate.
                    if (matchStart < 0) matchStart = j;
                    mutatedBases[j] = randomDNA.mutate(originalBases[i]);
                    if (Nucleotide.same(originalBases[i],mutatedBases[j])) {
                        throw new IllegalArgumentException("");
                    }
                    dotsMutatedBases[j] = (char) mutatedBases[j];
                    mutationRate = 0.2; // make it more likely to have a follow up mutation.
                    j++;
                } else {
                    indelRate = 0.01; // reset indel-rate.
                    if (matchStart < 0) matchStart = j;
                    dotsMutatedBases[j] = '.';
                    mutationRate = Math.max(0.01, mutationRate * .5);
                    j++;
                }
            }
            if (matchStart >= 0) cigar.add(new CigarElement(mutatedBases.length - matchStart, CigarOperator.M));
            final byte[] mutatedBasesFinal = mutatedBases;
            final byte[] contigSequence = ArrayUtils.removeAll(mutatedBases, IntStream.range(0, mutatedBases.length)
                     .filter(idx -> mutatedBasesFinal[idx] == '-')
                     .toArray());
            final AlignedContig chr1_101000_102000_with_mutations = new AlignedContig("ctg001", contigSequence, Collections.singletonList(new AlignmentInterval(chr1.getSequenceName() + ",101000,+," + new Cigar(cigar) + ",40,0,40")));
            result.add(new Object[]{chr1_101000_102000_with_mutations, 10, true,
                    '>' + chr1.getSequenceName() + ":100990-102010\n" + new String(referenceBases) + '\n' +
                            ">ctg001:1-1001\t" + chr1.getSequenceName() + ",101000,+," + new Cigar(cigar) + ",40,0,40\n" + StringUtils.repeat("-", 10) + new String(dotsMutatedBases) + StringUtils.repeat("-", 10)});

            return result.stream().toArray(Object[][]::new);
        } catch (final RuntimeException ex) {
            throw ex;
        }
    }
}
