package org.broadinstitute.hellbender.tools.spark.sv;

import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.ArgumentCollection;

import java.io.Serializable;

/**
 * Created by valentin on 6/1/18.
 */
public final class RealignmentScoreArgumentCollection implements Serializable {

    private static final long serialVersionUID = -1L;

    public static final double DEFAULT_MATCH_COST = 0.01;
    public static final double DEFAULT_MISMATCH_COST = 40;
    public static final double DEFAULT_GAP_OPEN_COST = 60;
    public static final double DEFAULT_GAP_EXTEND_COST = 10;
    public static final int DEFAULT_MIN_MAPQ = 16;

    public static final String GAP_OPEN_COST_PARAM_FULL_NAME = "realignment-gap-open-penalty";
    public static final String GAP_OPEN_COST_PARAM_SHORT_NAME = GAP_OPEN_COST_PARAM_FULL_NAME;
    public static final String GAP_EXTEND_COST_PARAM_FULL_NAME = "realignment-gap-extend-penalty";
    public static final String GAP_EXTEND_COST_PARAM_SHORT_NAME = GAP_EXTEND_COST_PARAM_FULL_NAME;
    public static final String MATCH_COST_PARAM_FULL_NAME = "realignment-match-penalty";
    public static final String MATCH_COST_PARAM_SHORT_NAME = MATCH_COST_PARAM_FULL_NAME;
    public static final String MISMATCH_COST_PARAM_FULL_NAME = "realignment-mismatch-penalty";
    public static final String MISMATCH_COST_PARAM_SHORT_NAME = MISMATCH_COST_PARAM_FULL_NAME;
    public static final String MINIMUM_MAP_QUALITY_PARAM_FULL_NAME = "realignment-minimum-mq";
    public static final String MINIMUM_MAP_QUALITY_PARAM_SHORT_NAME = MINIMUM_MAP_QUALITY_PARAM_FULL_NAME ;

   @Argument(fullName = GAP_OPEN_COST_PARAM_FULL_NAME, shortName = GAP_OPEN_COST_PARAM_SHORT_NAME,
            doc = "Phred-scaled cost for a gap (indel) opening in a read realigned against a contig or haplotype",
            optional =  true)
    public double gapOpenPenalty = DEFAULT_GAP_OPEN_COST;

    @Argument(fullName = GAP_EXTEND_COST_PARAM_FULL_NAME, shortName = GAP_EXTEND_COST_PARAM_SHORT_NAME,
            doc = "Phred-scaled cost for a gap (indel) extension in a read realigned against a contig or haplotype",
            optional = true)
    public double gapExtendPenalty = DEFAULT_GAP_EXTEND_COST;

    @Argument(fullName = MATCH_COST_PARAM_FULL_NAME, shortName = MATCH_COST_PARAM_SHORT_NAME,
            doc = "Phred-scaled cost for base match in a read realigned against a contig or haplotype",
            optional =  true)
    public double matchPenalty = DEFAULT_MATCH_COST;

    @Argument(fullName = MISMATCH_COST_PARAM_FULL_NAME, shortName = MISMATCH_COST_PARAM_SHORT_NAME,
            doc = "Phred-scaled cost for base mismatch in a read realigned against a contig or haplotype",
            optional =  true)
    public double mismatchPenalty = DEFAULT_MISMATCH_COST;

    @Argument(fullName = MINIMUM_MAP_QUALITY_PARAM_FULL_NAME, shortName = MINIMUM_MAP_QUALITY_PARAM_SHORT_NAME,
            doc = "Minimum mapping quality of a local read realignment interval to be considered",
            optional = true)
    public int minimumMappingQuality = DEFAULT_MIN_MAPQ;

    public RealignmentScoreArgumentCollection() {}
}
