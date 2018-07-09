package org.broadinstitute.hellbender.tools.walkers.annotator;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeaderLineType;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.utils.*;
import org.broadinstitute.hellbender.utils.genotyper.ReadLikelihoods;
import org.broadinstitute.hellbender.utils.variant.GATKVCFConstants;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;


public class DiscordantPairArtifact extends GenotypeAnnotation implements StandardMutectAnnotation{

    public static final String P_VAL_OA_TAG = "P_VAL_OA_TAG";

    @Override
    public void annotate(ReferenceContext ref, VariantContext vc, Genotype g, GenotypeBuilder gb, ReadLikelihoods<Allele> likelihoods) {
        Utils.nonNull(gb);
        Utils.nonNull(vc);
        Utils.nonNull(likelihoods);

        // do not annotate the genotype of the normal sample
        if (g.isHomRef()){
            return;
        }

        final double[] tumorLods = GATKProtectedVariantContextUtils.getAttributeAsDoubleArray(vc, GATKVCFConstants.TUMOR_LOD_KEY, () -> null, -1);
        final int indexOfMaxTumorLod = MathUtils.maxElementIndex(tumorLods);
        final Allele altAlelle = vc.getAlternateAllele(indexOfMaxTumorLod);
        final Allele refAllele = vc.getReference();

        Collection<ReadLikelihoods<Allele>.BestAllele> bestAlleles = likelihoods.bestAllelesBreakingTies(g.getSampleName());

        int discordantAlt = (int) bestAlleles.stream().filter(ba -> ba.read.hasAttribute("OA") && ba.isInformative() && ba.allele.equals(altAlelle)).count();
        int discordantRef = (int) bestAlleles.stream().filter(ba -> ba.read.hasAttribute("OA") && ba.isInformative() && ba.allele.equals(refAllele)).count();
        int concordantAlt = (int) bestAlleles.stream().filter(ba -> !ba.read.hasAttribute("OA") && ba.isInformative() && ba.allele.equals(altAlelle)).count();
        int concordantRef = (int) bestAlleles.stream().filter(ba -> !ba.read.hasAttribute("OA") && ba.isInformative() && ba.allele.equals(refAllele)).count();

        final int[][] contingencyTable = new int[2][2];
        contingencyTable[0][0] = discordantRef;
        contingencyTable[0][1] = concordantRef;
        contingencyTable[1][0] = discordantAlt;
        contingencyTable[1][1] = concordantAlt;

        double pVal = FisherExactTest.twoSidedPValue(contingencyTable);

        gb.attribute(P_VAL_OA_TAG, discordantAlt);
    }

    @Override
    public List<VCFFormatHeaderLine> getDescriptions() {
        return Arrays.asList(new VCFFormatHeaderLine(P_VAL_OA_TAG, 1, VCFHeaderLineType.Integer, "number of OA tag alt reads"));

    }

    @Override
    public List<String> getKeyNames() {
        return Arrays.asList(P_VAL_OA_TAG);
    }
}
