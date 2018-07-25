package org.broadinstitute.hellbender.tools.spark.sv;

import htsjdk.variant.variantcontext.GenotypeLikelihoods;
import org.apache.commons.collections4.CollectionUtils;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVHaplotype;
import org.broadinstitute.hellbender.tools.walkers.genotyper.GenotypeLikelihoodCalculator;
import org.broadinstitute.hellbender.tools.walkers.genotyper.GenotypeLikelihoodCalculators;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.genotyper.IndexedAlleleList;
import org.broadinstitute.hellbender.utils.genotyper.LikelihoodMatrix;
import org.broadinstitute.hellbender.utils.genotyper.ReadLikelihoods;
import org.broadinstitute.hellbender.utils.genotyper.SampleList;
import org.broadinstitute.hellbender.utils.haplotype.Haplotype;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by valentin on 5/18/17.
 */
public class TemplateHaplotypeScoreTable implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TemplateMappingInformation[][] mappingInfo;

    private final double[][] bestMappingScorePerFragment;
    private final double[][] bestMappingFragmentMQ;
    private final double[][] worstMappingScorePerFragment;
    private final double[][] worstMappingScoresFragmentMQ;
    private boolean needsTocalculateBestMappingScores = true;

    private final double[][] values;

    private final List<Template> templates;

    private final Map<String, Integer> templateIndex;

    private final List<SVHaplotype> haplotypes;

    public int maximumInsertSize() {
        int result = 0;
        for (int i = 0; i < mappingInfo.length; i++) {
            for (int j = 0; j < mappingInfo[i].length; j++) {
                if (mappingInfo[i][j] != null && mappingInfo[i][j].insertSize.isPresent() && mappingInfo[i][j].insertSize.getAsInt() > result) {
                    result =  mappingInfo[i][j].insertSize.getAsInt();
                }
            }
        }
        return result;
    }

    public TemplateHaplotypeScoreTable(final Iterable<Template> templates, final Iterable<SVHaplotype> haplotypes)
    {
        this.templates = CollectionUtils.collect(templates, t -> t, new ArrayList<>(1000));
        this.haplotypes = CollectionUtils.collect(haplotypes, t -> t, new ArrayList<>());
        values = new double[this.haplotypes.size()][this.templates.size()];
        mappingInfo = new TemplateMappingInformation[this.haplotypes.size()][this.templates.size()];
        bestMappingScorePerFragment = new double[this.templates.size()][2];
        bestMappingFragmentMQ = new double[this.templates.size()][2];
        worstMappingScorePerFragment = new double[this.templates.size()][2];
        worstMappingScoresFragmentMQ = new double[this.templates.size()][2];
        this.templateIndex = composeTemplateIndex(this.templates);
    }

    private Map<String,Integer> composeTemplateIndex(final List<Template> templates) {
        final Map<String, Integer> result = new HashMap<>(templates.size());
        for (int i = 0; i < templates.size(); i++) {
            result.put(templates.get(i).name(), i);
        }
        return result;
    }

    public double get(final SVHaplotype allele, final Template template) {
        return get(indexOf(allele), indexOf(template));
    }

    public int indexOf(final Template template) {
        Utils.nonNull(template);
        return templateIndex.getOrDefault(template.name(), -1);
    }

    public int indexOf(final SVHaplotype allele) {
        Utils.nonNull(allele);
        int i = 0;
        while (i < haplotypes.size()) {
            if (Arrays.equals(haplotypes.get(i).getBases(), allele.getBases())) {
                return i;
            }
            i++;
        }
        return -1;
    }

    public double get(final int alleleIndex, final int templateIndex) {
        Utils.validIndex(alleleIndex, haplotypes.size());
        Utils.validIndex(templateIndex, templates.size());
        return values[alleleIndex][templateIndex];
    }

    public void set(final int alleleIndex, final int templateIndex, final double value) {
        Utils.validIndex(alleleIndex, haplotypes.size());
        Utils.validIndex(templateIndex, templates.size());
        values[alleleIndex][templateIndex] = value;
    }

    public TemplateMappingInformation getMappingInfo(final int alleleIndex, final int templateIndex) {
        Utils.validIndex(alleleIndex, haplotypes.size());
        Utils.validIndex(templateIndex, templates.size());
        return mappingInfo[alleleIndex][templateIndex];
    }

    public void setMappingInfo(final int alleleIndex, final int templateIndex, final TemplateMappingInformation mappingInformation) {
        Utils.validIndex(alleleIndex, haplotypes.size());
        Utils.validIndex(templateIndex, templates.size());
        mappingInfo[alleleIndex][templateIndex] = mappingInformation;
    }

    public int numberOfTemplates() {
        return templates.size();
    }

    public int numberOfHaplotypes() {
        return haplotypes.size();
    }

    public List<Template> templates() {
        return templates;
    }

    public List<SVHaplotype> haplotypes() {
        return haplotypes;
    }


    public int[] informativeTemplateIndexes() {
        return IntStream.range(0, templates.size())
                .filter(i -> {
                    final double firstLikelihood = values[0][i];
                    if (Double.isNaN(firstLikelihood)) {
                        return false;
                    } else {
                        boolean foundDifference = false;
                        for (int j = 1; j < values.length; j++) {
                            if (Double.isNaN(values[j][i])) {
                                return false;
                            } else if (values[j][i] != firstLikelihood) {
                                foundDifference = true;
                            }
                        }
                        return foundDifference;
                    }
                }).toArray();
    }

    public void dropUninformativeTemplates() {
        final int[] informativeIndexes = informativeTemplateIndexes();
        if (informativeIndexes.length == 0) {
            templates.clear();
            templateIndex.clear();
            for (int j = 0; j < values.length; j++) {
                values[j] = new double[0];
            }
        } else if (informativeIndexes.length != templates.size()) {
            final List<Template> newTemplates = new ArrayList<>(informativeIndexes.length);
            templateIndex.clear();
            for (final int informativeIndexe : informativeIndexes) {
                newTemplates.add(templates.get(informativeIndexe));
                templateIndex.put(newTemplates.get(newTemplates.size() - 1).name(), newTemplates.size() - 1);
            }
            templates.clear();
            templates.addAll(newTemplates);
            for (int j = 0; j < haplotypes.size(); j++) {
                final double[] newValues = new double[informativeIndexes.length];
                final TemplateMappingInformation[] newMappingInfo = new TemplateMappingInformation[informativeIndexes.length];
                for (int i = 0; i < informativeIndexes.length; i++) {
                    newValues[i] = values[j][informativeIndexes[i]];
                    newMappingInfo[i] = mappingInfo[j][informativeIndexes[i]];
                }
                values[j] = newValues;
                mappingInfo[j] = newMappingInfo;
            }
        } // else {...} no changes.
    }

    public double[] getRow(final int i) {
        return values[Utils.validIndex(i, values.length)].clone();
    }

    public void calculateBestMappingScores() {
        if (!needsTocalculateBestMappingScores) {
            return;
        }
        for (int i = 0; i < templates.size(); i++) {
            for (int j = 0; j < 2; j++) {
                double best = Double.NaN;
                double bestMq = Double.NaN;
                double worst = Double.NaN;
                double worstMq = Double.NaN;
                for (int k = 0; k < haplotypes.size(); k++) {
                    final OptionalDouble score = (j == 0 ? (mappingInfo[k][i].firstAlignmentScore) : (mappingInfo[k][i].secondAlignmentScore));
                    if (score.isPresent() && (Double.isNaN(best) || score.getAsDouble() > best)) {
                        best = score.getAsDouble();
                        if (haplotypes.get(k).isContig()) {
                            bestMq = haplotypes.get(k).mappingQuality();
                        }
                    }
                    if (score.isPresent() && (Double.isNaN(worst) || score.getAsDouble() < worst)) {
                        worst = score.getAsDouble();
                        if (haplotypes.get(k).isContig()) {
                            worstMq = haplotypes.get(k).mappingQuality();
                        }
                    }
                }
                bestMappingScorePerFragment[i][j] = best;
                bestMappingFragmentMQ[i][j] = bestMq;
                worstMappingScorePerFragment[i][j] = worst;
                worstMappingScoresFragmentMQ[i][j] = worstMq;
            }
        }
        needsTocalculateBestMappingScores = false;
    }

    public OptionalDouble getWorstAlignmentScore(final int templateIndex, final int fragmentIndex) {
        calculateBestMappingScores();
        final double result = worstMappingScorePerFragment[templateIndex][fragmentIndex];
        return Double.isNaN(result) ? OptionalDouble.empty() : OptionalDouble.of(result);
    }

    public OptionalDouble getBestAlignmentScore(final int templateIndex, final int fragmentIndex) {
        calculateBestMappingScores();
        final double result = bestMappingScorePerFragment[templateIndex][fragmentIndex];
        return Double.isNaN(result) ? OptionalDouble.empty() : OptionalDouble.of(result);
    }


    public void applyMissingAlignmentScore(final int template, final int fragment, final double missingAlignmentScore) {
        final OptionalDouble score = OptionalDouble.of(missingAlignmentScore);
        for (int h = 0; h < haplotypes.size(); h++) {
            if (fragment == 0 && !getMappingInfo(h, template).firstAlignmentScore.isPresent())
                getMappingInfo(h, template).firstAlignmentScore = score;
            else if ( fragment == 1 && !getMappingInfo(h, template).secondAlignmentScore.isPresent())
                getMappingInfo(h, template).secondAlignmentScore = score;
        }
    }

    public String toString(final InsertSizeDistribution distr, final int[] refBreakPoints, final int[] altBreakPoints, final Set<String> filterDown) {
        final StringBuilder builder = new StringBuilder((400 * templates.size()) + 20 * (templates.size() * haplotypes().size()));
        builder.append("template");
        for (final SVHaplotype haplotype : haplotypes()) {
            builder.append('\t').append(haplotype.getName());
            if (haplotype.isContig()) {
                final SVContig contig = (SVContig) haplotype;
                final double refScore = contig.getReferenceScore();
                final double altScore = contig.getAlternativeScore();
                final String call = refScore < altScore ? "alt" : ((altScore < refScore) ? "ref" : ".");
                final double qual = call.equals("alt") ? (altScore - refScore) : (call.equals("ref") ? refScore - altScore : Double.NaN);
                builder.append(':').append(call).append(Double.isNaN(qual) ? "" : String.format(":%.2f", qual));
            }
        }
        for (final Template template : templates) {
            final int templateIndex = indexOf(template);
            if (filterDown != null) {
                if (!filterDown.contains(template.name())) {
                    continue;
                }
            }
            builder.append('\n').append(template.name());
            for (final SVHaplotype haplotype : haplotypes()) {
                final int haplotypeIndex = indexOf(haplotype);
                builder.append('\t');
                final TemplateMappingInformation info = getMappingInfo(haplotypeIndex, templateIndex);
                builder.append(String.format("%.2f", get(haplotypeIndex, templateIndex)))
                        .append(':').append(info.pairOrientation)
                        .append(':').append(info.insertSize.orElse(-1))
                        .append(':').append(String.format("%.2f", info.insertSize.isPresent() ? distr.logProbability(info.insertSize.getAsInt()) : Double.NaN))
                        .append(':').append(String.format("%.2f", info.firstAlignmentScore.orElse(Double.NaN)))
                        .append(':').append(String.format("%.2f", info.secondAlignmentScore.orElse(Double.NaN)));
                if (haplotype.getName().equals("ref")) {
                    builder.append(':').append(info.crossesBreakPoint(refBreakPoints) ? "1" : "0");
                } else if (haplotype.getName().equals("alt")) {
                    builder.append(':').append(info.crossesBreakPoint(altBreakPoints) ? "1" : "0");
                }
            }
        }
        return builder.toString();
    }
}
