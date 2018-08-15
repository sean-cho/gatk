package org.broadinstitute.hellbender.tools.copynumber.allelic.alleliccount;

import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.Nucleotide;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.tsv.DataLine;
import org.broadinstitute.hellbender.utils.tsv.TableReader;

import java.io.File;
import java.io.IOException;

/**
 * Reads {@link AllelicCount} instances from a tab-separated file.
 * All {@link AllelicCount} fields must be specified (including ref/alt nucleotide).
 *
 * @author Mehrtash Babadi &lt;mehrtash@broadinstitute.org&gt;
 * @author Samuel Lee &lt;slee@broadinstitute.org&gt;
 */
final class AllelicCountReader extends TableReader<AllelicCount> {

    AllelicCountReader(final File file) throws IOException {
        super(file);
    }

    @Override
    protected AllelicCount createRecord(final DataLine dataLine) {
        try {
            final String contig = dataLine.get(AllelicCountTableColumn.CONTIG);
            final int position = dataLine.getInt(AllelicCountTableColumn.POSITION);
            final int refReadCount = dataLine.getInt(AllelicCountTableColumn.REF_COUNT);
            final int altReadCount = dataLine.getInt(AllelicCountTableColumn.ALT_COUNT);
            final Nucleotide refNucleotide = Nucleotide.decode(dataLine.get(AllelicCountTableColumn.REF_NUCLEOTIDE.name()).getBytes()[0]);
            final Nucleotide altNucleotide = Nucleotide.decode(dataLine.get(AllelicCountTableColumn.ALT_NUCLEOTIDE.name()).getBytes()[0]);
            final SimpleInterval interval = new SimpleInterval(contig, position, position);
            return new AllelicCount(interval, refReadCount, altReadCount, refNucleotide, altNucleotide);
        } catch (final IllegalArgumentException e) {
            throw new UserException.BadInput("AllelicCountCollection file must have all columns specified.");
        }
    }
}
