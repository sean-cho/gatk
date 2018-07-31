package org.broadinstitute.hellbender.tools.spark.sv;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMProgramRecord;
import htsjdk.samtools.SAMReadGroupRecord;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.util.Locatable;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.GenotypeLikelihoods;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLine;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.apache.logging.log4j.LogManager;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.ArgumentCollection;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.argumentcollections.RequiredVariantInputArgumentCollection;
import org.broadinstitute.hellbender.cmdline.programgroups.StructuralVariantDiscoveryProgramGroup;
import org.broadinstitute.hellbender.engine.Shard;
import org.broadinstitute.hellbender.engine.TraversalParameters;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.engine.spark.SparkSharder;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSource;
import org.broadinstitute.hellbender.engine.spark.datasources.VariantsSparkSource;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.alignment.AlignmentInterval;
import org.broadinstitute.hellbender.tools.spark.sv.utils.ArraySVHaplotype;
import org.broadinstitute.hellbender.tools.spark.sv.utils.GATKSVVCFConstants;
import org.broadinstitute.hellbender.tools.spark.sv.utils.GATKSVVCFHeaderLines;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVContextBuilder;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVHaplotype;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVIntervalLocator;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVIntervalTree;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVVCFWriter;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVContext;
import org.broadinstitute.hellbender.tools.walkers.genotyper.GenotypeLikelihoodCalculator;
import org.broadinstitute.hellbender.tools.walkers.genotyper.GenotypeLikelihoodCalculators;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.MathUtils;
import org.broadinstitute.hellbender.utils.SerializableBiFunction;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.bwa.BwaMemAligner;
import org.broadinstitute.hellbender.utils.bwa.BwaMemAlignment;
import org.broadinstitute.hellbender.utils.bwa.BwaMemAlignmentUtils;
import org.broadinstitute.hellbender.utils.bwa.BwaMemIndex;
import org.broadinstitute.hellbender.utils.bwa.BwaMemIndexCache;
import org.broadinstitute.hellbender.utils.gcs.BamBucketIoUtils;
import org.broadinstitute.hellbender.utils.genotyper.LikelihoodMatrix;
import org.broadinstitute.hellbender.utils.genotyper.ReadLikelihoods;
import org.broadinstitute.hellbender.utils.read.CigarUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.reference.FastaReferenceWriter;
import org.broadinstitute.hellbender.utils.variant.GATKVariantContextUtils;
import picard.util.MathUtil;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.function.IntFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Created by valentin on 4/20/17.
 */
@CommandLineProgramProperties(summary = "genotype SV variant call files",
        oneLineSummary = "genotype SV variant call files",
        programGroup = StructuralVariantDiscoveryProgramGroup.class)
public class GenotypeStructuralVariantsSpark extends GATKSparkTool {

    public static final String FASTQ_FILE_DIR_SHORT_NAME = "fastqDir";
    public static final String FASTQ_FILE_DIR_FULL_NAME = "fastqAssemblyDirectory";
    public static final String HAP_AND_CTG_FILE_SHORT_NAME = "assemblies";
    public static final String HAP_AND_CTG_FILE_FULL_NAME = "haplotypesAndContigsFile";
    public static final String OUTPUT_ALIGNMENT_SHORT_NAME = "outputAlignment";
    public static final String OUTPUT_ALIGNMENT_FULL_NAME = "outputAlignmentFile";
    public static final String SHARD_SIZE_SHORT_NAME = "shard";
    public static final String SHARD_SIZE_FULL_NAME = "shardSize";
    public static final String INSERT_SIZE_DISTR_SHORT_NAME = "insSize";
    public static final String INSERT_SIZE_DISTR_FULL_NAME = "insertSizeDistribution";
    public static final String UNKNOWN_SAMPLE_NAME = "<UNKNOWN>";
    public static final String EMIT_GENOTYPING_PERFORMANCE_STATS_SHORT_NAME = "emitGenotypingStats";
    public static final String EMIT_GENOTYPING_PERFORMANCE_STATS_FULL_NAME = EMIT_GENOTYPING_PERFORMANCE_STATS_SHORT_NAME;
    public static final String EMIT_STRATIFIED_LIKELIHOODS_SHORT_NAME = "emitStratifiedLikelihoods";
    public static final String EMIT_STRATIFIED_LIKELIHOODS_FULL_NAME = EMIT_STRATIFIED_LIKELIHOODS_SHORT_NAME;
    public static final String EMIT_STRATIFIED_ALLELE_DEPTHS_SHORT_NAME = "emitStratifiedAlleleDepthps";
    public static final String EMIT_STRATIFIED_ALLELE_DEPTHS_FULL_NAME = EMIT_STRATIFIED_ALLELE_DEPTHS_SHORT_NAME;

    private static final long serialVersionUID = 1L;
    public static final int MAX_NUMBER_OF_TEMPLATES_IN_CONTEXT = 5000;

    @ArgumentCollection
    private RequiredVariantInputArgumentCollection variantArguments = new RequiredVariantInputArgumentCollection();

    @ArgumentCollection
    private RealignmentScoreParameters realignmentScoreArguments = new RealignmentScoreParameters();

    @Argument(doc = "fastq files location",
            shortName = FASTQ_FILE_DIR_SHORT_NAME,
            fullName = FASTQ_FILE_DIR_FULL_NAME)
    private String fastqDir = null;

    @Argument(doc = "assemblies SAM/BAM file location",
            shortName = HAP_AND_CTG_FILE_SHORT_NAME,
            fullName = HAP_AND_CTG_FILE_FULL_NAME)
    private String haplotypesAndContigsFile = null;

    @Argument(doc = "assemblies SAM/BAM file aligned",
            shortName = OUTPUT_ALIGNMENT_SHORT_NAME,
            fullName = OUTPUT_ALIGNMENT_FULL_NAME,
            optional = true)
    private String outputAlignmentFile = null;

    @Argument(doc = "output VCF file",
            shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
            optional = true)
    private String outputFile = null;

    @Argument(doc = "sample name",
            shortName = StandardArgumentDefinitions.SAMPLE_NAME_SHORT_NAME,
            fullName = StandardArgumentDefinitions.SAMPLE_NAME_LONG_NAME,
            optional = true)
    private String sampleName = null;

    @Argument(doc = "include in the output VCF genotyping performance (cpu-time) relevant annotations such as number of templates, haplotype/contigs involved and genotyping elapse time",
              shortName = EMIT_GENOTYPING_PERFORMANCE_STATS_SHORT_NAME,
              fullName = EMIT_GENOTYPING_PERFORMANCE_STATS_FULL_NAME,
              optional = true)
    private boolean emitGenotypingPerformanceStats = false;

    @Argument(doc = "include in the output VCF stratified likelihoods based on mapping, insert-size and discordant pair-orientation",
              shortName = EMIT_STRATIFIED_LIKELIHOODS_SHORT_NAME,
              fullName = EMIT_STRATIFIED_LIKELIHOODS_FULL_NAME,
              optional = true)
    private boolean emitStratifiedLikelihoods = false;

    @Argument(doc = "include in the output VCF stratified allele depths based on mapping, insert-size and discordant pair-orientation",
              shortName = EMIT_STRATIFIED_ALLELE_DEPTHS_SHORT_NAME,
              fullName = EMIT_STRATIFIED_ALLELE_DEPTHS_FULL_NAME,
              optional = true)
    private boolean emitStratifiedAlleleDepths = false;

    @SuppressWarnings("FieldCanBeLocal")
    @Argument(doc = "shard size",
            shortName = SHARD_SIZE_SHORT_NAME,
            fullName = SHARD_SIZE_FULL_NAME,
            optional = true)
    private int shardSize = 100000;

    @SuppressWarnings("FieldCanBeLocal")
    @Argument(doc = "parallelism factor", shortName = "pfactor", fullName = "parallelismFactor", optional = true)
    private int parallelismFactor = 4;

    @SuppressWarnings("FieldCanBeLocal")
    @Argument(doc = "minimum likelihood (Phred) difference to consider that a template or read support an allele over any other",
            shortName = "infoTLD", fullName = "informativeTemplateLikelihoodDifference", optional = true)
    private double informativeTemplateDifferencePhred = 2.0;

    @Argument(doc = "insert size distribution",
            shortName = INSERT_SIZE_DISTR_SHORT_NAME,
            fullName = INSERT_SIZE_DISTR_FULL_NAME,
            optional = true)
    private InsertSizeDistribution insertSizeDistribution = new InsertSizeDistribution("N(309,149)");

    private VariantsSparkSource variantsSource;
    private ReadsSparkSource haplotypesAndContigsSource;
    public static final Pattern ASSEMBLY_NAME_ALPHAS = Pattern.compile("[a-zA-Z_]+");
    private int parallelism = 0;

    @Override
    public boolean requiresReads() {
        return false;
    }

    @Override
    public boolean requiresReference() {
        return true;
    }

    private void setUp(final JavaSparkContext ctx) {

        parallelism = ctx.defaultParallelism() * parallelismFactor;
        variantsSource = new VariantsSparkSource(ctx);
        haplotypesAndContigsSource = new ReadsSparkSource(ctx);
    }

    private static class Localized<E> implements Locatable, Serializable {

        private static final long serialVersionUID = 1L;
        private final Locatable location;
        private final E element;

        public Localized(final E element, final Locatable location) {
            this.location = location;
            this.element = element;
        }

        public String getContig() {
            return location.getContig();
        }

        public int getStart() {
            return location.getStart();
        }

        public int getEnd() {
            return location.getEnd();
        }

        public E get() {
            return element;
        }

        @SuppressWarnings("unchecked")
        public static <E> Class<Localized<E>> getSubClass(final Class<E> elementClass) {
            return (Class<Localized<E>>) (Class<?>) Localized.class;
        }
    }

    @Override
    protected void runTool(final JavaSparkContext ctx) {
        setUp(ctx);
        final RealignmentScoreParameters realignmentScoreArguments = this.realignmentScoreArguments;
        final List<SimpleInterval> intervals = hasIntervals() ? getIntervals()
                : IntervalUtils.getAllIntervalsForReference(getReferenceSequenceDictionary());
        final TraversalParameters traversalParameters = new TraversalParameters(intervals, false);
        final JavaRDD<SVHaplotype> haplotypeAndContigs = haplotypesAndContigsSource
                .getParallelReads(haplotypesAndContigsFile, referenceArguments.getReferenceFileName(), traversalParameters)
                .repartition(parallelism)
                .map(r -> r.getReadGroup().equals("CTG") ? SVContig.of(r, realignmentScoreArguments) : ArraySVHaplotype.of(r));
        final JavaRDD<SVContext> variants = variantsSource.getParallelVariantContexts(
                variantArguments.variantFiles.get(0).getFeaturePath(), getIntervals())
                .repartition(parallelism)
                .map(SVContext::of).filter(GenotypeStructuralVariantsSpark::structuralVariantAlleleIsSupported);
        final SparkSharder sharder = new SparkSharder(ctx, getReferenceSequenceDictionary(), intervals, shardSize, 0);

        final JavaRDD<Shard<Localized<SVContext>>> variantSharded = sharder.shard(variants.map(v -> new Localized<>(v, new SimpleInterval(v.getContig(), v.getStart(), v.getStart()))));
        final JavaRDD<Shard<Localized<SVHaplotype>>> haplotypesSharded = sharder.shard(haplotypeAndContigs.map(h -> new Localized<>(h, h.getVariantLocation())));
        final JavaPairRDD<Shard<Localized<SVContext>>, Shard<Localized<SVHaplotype>>> variantAndHaplotypesSharded = sharder.cogroup(variantSharded, haplotypesSharded);
        final JavaPairRDD<Localized<SVContext>, Iterable<Localized<SVHaplotype>>> variantAndHaplotypesLocalized = sharder
                .matchLeftByKey(variantAndHaplotypesSharded, x -> x.get().getUniqueID(), x -> x.get().getVariantId());
        final JavaPairRDD<SVContext, List<SVHaplotype>> variantAndHaplotypes = variantAndHaplotypesLocalized
                .mapToPair(tuple -> new Tuple2<>(tuple._1().get(), Utils.stream(tuple._2().iterator()).map(Localized::get).collect(Collectors.toList())));

        final String fastqDir = this.fastqDir;
        final String fastqFileFormat = "asm%06d.fastq";
        final Broadcast<SAMSequenceDictionary> dictionaryBroadcast = ctx.broadcast(getReferenceSequenceDictionary());
        final Broadcast<SVIntervalLocator> locatorBroadcast = ctx.broadcast(SVIntervalLocator.of(getReferenceSequenceDictionary()));
        final Broadcast<InsertSizeDistribution> insertSizeDistributionBroadcast = ctx.broadcast(insertSizeDistribution);
        final VCFHeader outputVCFHeader = composeOutputVCFHeader(VariantsSparkSource.getHeader(variantArguments.variantFiles.get(0).getFeaturePath()), sampleName, emitGenotypingPerformanceStats,
                emitStratifiedLikelihoods, emitStratifiedAlleleDepths);
        final String sampleName = outputVCFHeader.getSampleNamesInOrder().get(0);
        final JavaRDD<SVGenotypingContext> variantHaplotypesAndTemplates =
                variantAndHaplotypes.mapPartitions(it -> {
                    final SAMSequenceDictionary dictionary = dictionaryBroadcast.getValue();
                    final AssemblyCollection assemblyCollection = new AssemblyCollection(fastqDir, fastqFileFormat);
                    final SVIntervalLocator locator = locatorBroadcast.getValue();
                    final InsertSizeDistribution insertSizeDistribution = insertSizeDistributionBroadcast.getValue();
                    return Utils.stream(it)
                            .filter(tuple ->
                                tuple._2().size() >= 2)
                            .map(tuple -> {
                                final SVContext variant = tuple._1();
                                final List<SVHaplotype> haplotypes = tuple._2();
                                return composeGenotypingContext(variant, sampleName, haplotypes, dictionary, assemblyCollection, locator, insertSizeDistribution);
                            }).iterator();
                });

        final SAMFileHeader outputAlignmentHeader = outputAlignmentFile == null ? null : composeOutputHeader(getReferenceSequenceDictionary());
        final JavaRDD<Call> calls = processVariants(variantHaplotypesAndTemplates, outputAlignmentHeader, ctx);
        SVVCFWriter.writeVCF(outputFile, referenceArguments.getReferenceFileName(), calls.map(c -> c.context), outputVCFHeader, logger);
        if (outputAlignmentFile != null) {
            try (final SAMFileWriter outputAlignmentWriter = BamBucketIoUtils.makeWriter(outputAlignmentFile, outputAlignmentHeader, false)) {
                calls.flatMap(c -> c.outputAlignmentRecords.iterator()).toLocalIterator().forEachRemaining((read) -> {
                    final SAMRecord record = read.convertToSAMRecord(outputAlignmentHeader);
                    outputAlignmentWriter.addAlignment(record);
                });
            } catch (final Exception ex) {
                throw new UserException.CouldNotCreateOutputFile(outputAlignmentFile, ex);
            }
        }
        tearDown(ctx);
    }

    /**
     * Given a variant and the list of relevant haplotypes composes the genotyping context object.
     * <p>
     *     For that it needs to gather all the relevant templates (fragment/read pairs) that were
     *     use on the assemblies associated to the haplotypes.
     * </p>
     */
    private static SVGenotypingContext composeGenotypingContext(final SVContext variant, final String sampleName,
                                                                final List<SVHaplotype> haplotypes,
                                                                final SAMSequenceDictionary dictionary,
                                                                final AssemblyCollection assemblyCollection,
                                                                final SVIntervalLocator locator,
                                                                final InsertSizeDistribution insertSizeDistribution) {
        final SVIntervalTree<SimpleInterval> coveredReference = haplotypes.stream()
                .filter(h -> !h.isNeitherReferenceNorAlternative())
                .flatMap(h -> h.getReferenceAlignmentIntervals().stream())
                .flatMap(ai -> ai.referenceCoveredIntervals().stream())
                .collect(locator.toTreeCollector(si -> si));
        final IntStream assemblyNumbers = haplotypes.stream()
                .filter(SVHaplotype::isNeitherReferenceNorAlternative)
                .map(SVHaplotype::getName)
                .map(n -> n.substring(0, n.indexOf(":")))
                .map(a -> ASSEMBLY_NAME_ALPHAS.matcher(a).replaceAll("0"))
                .mapToInt(Integer::parseInt)
                .sorted()
                .distinct();
        final List<Template> allTemplates = assemblyNumbers
                .boxed()
                .flatMap(i -> assemblyCollection.templates(i).stream())
                .distinct()
                .peek(template -> template.calculateMaximumMappingQualities(coveredReference, locator, insertSizeDistribution))
                .collect(Collectors.toList());
        return new SVGenotypingContext(variant, haplotypes, allTemplates, sampleName, dictionary);
    }

    private SAMFileHeader composeOutputHeader(final SAMSequenceDictionary dictionary) {
        final SAMFileHeader inputHeader = this.getHeaderForReads();
        final SAMFileHeader outputHeader = inputHeader.clone();
        final List<String> inputSamples = inputHeader.getReadGroups().stream().map(SAMReadGroupRecord::getSample).filter(Objects::nonNull).distinct().collect(Collectors.toList());
        final String hapAndCtgSample = inputSamples.size() == 1 ? inputSamples.get(0) : UNKNOWN_SAMPLE_NAME;
        final SAMProgramRecord programRecord = new SAMProgramRecord(getProgramName());
        programRecord.setCommandLine(getCommandLine());
        outputHeader.setSequenceDictionary(dictionary);
        outputHeader.addProgramRecord(programRecord);
        outputHeader.setSortOrder(SAMFileHeader.SortOrder.coordinate);
        final SAMReadGroupRecord contigsReadGroup = new SAMReadGroupRecord(ComposeStructuralVariantHaplotypesSpark.CONTIG_READ_GROUP);
        contigsReadGroup.setSample(hapAndCtgSample);
        final SAMReadGroupRecord haplotypesReadGroup = new SAMReadGroupRecord(ComposeStructuralVariantHaplotypesSpark.HAPLOTYPE_READ_GROUP);
        haplotypesReadGroup.setSample(hapAndCtgSample);
        outputHeader.addReadGroup(haplotypesReadGroup);
        outputHeader.addReadGroup(contigsReadGroup);
        return outputHeader;
    }

    private VCFHeader composeOutputVCFHeader(final VCFHeader inputHeader, final String sampleName,
                                          final boolean includePerformanceAnnotations,
                                          final boolean includeStratifiedLikelihoods,
                                          final boolean includeStratifiedAlleleDepths) {
        final List<String> sampleNames = inputHeader.getSampleNamesInOrder();
        final List<String> sampleNameList = Collections.singletonList(sampleName == null && sampleNames.isEmpty() ? "<UNKNOWN>" : (sampleName == null) ?sampleNames.get(0) : sampleName);
        final Set<VCFHeaderLine> inputHeaderLines = inputHeader.getMetaDataInInputOrder();
        final Collection<VCFHeaderLine> genotypingLines = GATKSVVCFHeaderLines.genotypingHeaderLines(includePerformanceAnnotations, includeStratifiedLikelihoods, includeStratifiedAlleleDepths);
        final Set<VCFHeaderLine> outputHeaderLines = new LinkedHashSet<>(inputHeaderLines.size() + 20);
        outputHeaderLines.addAll(inputHeaderLines);
        outputHeaderLines.addAll(genotypingLines);
        outputHeaderLines.add(new VCFInfoHeaderLine(VCFConstants.END_KEY, 1, VCFHeaderLineType.Integer, "last base position of the variant"));
        return new VCFHeader(outputHeaderLines, sampleNameList);
    }

    private static File createTransientImageFile(final String name, final byte[] bases) {
        try {
            final File fasta = File.createTempFile(name, ".fasta");
            final File image = File.createTempFile(name, ".img");
            FastaReferenceWriter.writeSingleSequenceReference(fasta.toPath(), false, false, name, "", bases);
            BwaMemIndex.createIndexImageFromFastaFile(fasta.toString(), image.toString());
            if (!fasta.delete()) {
                LogManager.getLogger(GenotypeStructuralVariantsSpark.class)
                        .warn("Could not remove temporary fasta file: " + fasta.toString());
            }
            image.deleteOnExit();
            return image;
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private static class Call implements Serializable {
        private static final long serialVersionUID = -1L;
        public final SVContext context;
        public final List<GATKRead> outputAlignmentRecords;

        private Call(final SVContext context, final List<GATKRead> outputAlignmentRecords) {
            this.context = context;
            this.outputAlignmentRecords = outputAlignmentRecords;
        }

        public static Call of(final SVContext context) {
            return new Call(context, Collections.emptyList());
        }

    }

    @SuppressWarnings("unused")
    private static Object[] debug00(final LikelihoodMatrix<?> left, final LikelihoodMatrix<?> right) {
        final Object[] result = new Object[left.numberOfReads()];
        for (int i = 0; i < result.length; i++) {
            final double one = left.get(0, i) + right.get(0, i);
            final double two = left.get(1, i) + right.get(1, i);
            result[i] = StringUtils.join(new Object[]{left.get(0, i), right.get(0, i), left.get(1, i), right.get(1, i), left.get(0, i) + right.get(0, i),
                    left.get(1, i) + right.get(1, i), one - two, (one > two) ? -Math.min(0, two - 2 * one) : Math.min(0, one - 2 * two)}, ",");
        }
        return result;
    }

    private JavaRDD<Call> processVariants(final JavaRDD<SVGenotypingContext> input, final SAMFileHeader outputAlignmentHeader, final JavaSparkContext ctx) {
        final Broadcast<SAMSequenceDictionary> broadCastDictionary = ctx.broadcast(getReferenceSequenceDictionary());
        final SerializableBiFunction<String, byte[], File> imageCreator = GenotypeStructuralVariantsSpark::createTransientImageFile;
        final InsertSizeDistribution insertSizeDistribution = this.insertSizeDistribution;
        final RealignmentScoreParameters realignmentScoreArguments = this.realignmentScoreArguments;
        final boolean emitGenotypingPerformanceStats = this.emitGenotypingPerformanceStats;
        final boolean emitStratifiedAlleleDepths = this.emitStratifiedAlleleDepths;
        final boolean emitStratifiedLikelihoods = this.emitStratifiedLikelihoods;
        final double informativePhredLikelihoodDifferenceThreshold = this.informativeTemplateDifferencePhred;
        return input.mapPartitions(it -> {
            final SAMSequenceDictionary dictionary = broadCastDictionary.getValue();
            final Stream<SVGenotypingContext> contexts =
                    Utils.stream(it);
            final Map<String, File> imagesByName = new HashMap<>();
            final SAMFileHeader header = new SAMFileHeader();
            header.setSequenceDictionary(dictionary);
            final GenotypeLikelihoodCalculator genotypeCalculator =
                    new GenotypeLikelihoodCalculators().getInstance(2, 2);

            return contexts
                    .sequential()
                    .filter(genotypingContext -> genotypingContext.numberOfTemplates > 0)
                    .map(context -> {
                        final SVContextBuilder outputBuilder = new SVContextBuilder(context.variant);
                        if (emitGenotypingPerformanceStats) {
                            outputBuilder.setGenotypingContextSizes(context.numberOfTemplates, context.numberOfHaplotypes);
                            outputBuilder.startRecordingProcessingTime();
                        }

                        adjustLikelihoodCalculatorAlleleFrequencies(context, insertSizeDistribution, genotypeCalculator);

                        context.reduceNumberOfTemplatesTo(MAX_NUMBER_OF_TEMPLATES_IN_CONTEXT);
                        final List<SVHaplotype> haplotypes = context.haplotypes;
                        final List<Template> templates = context.templates;

                        final TemplateMappingTable scoreTable = remapTemplatesOnHaplotypes(context, imageCreator, realignmentScoreArguments, imagesByName);
                        setMappingQualityToZeroForFragmentsThatMapDifferentlyOnFullReference(context, scoreTable);
                        setMissingAlignmentScores(realignmentScoreArguments, templates, scoreTable);

                        // we check what templates are relevant toward genotyping, by default only those that map across a break point.

                        final ReadLikelihoods<SVGenotypingContext.Allele> splitsReadlikelihoods = calculateSplitReadLikelihoods(context, realignmentScoreArguments, scoreTable);
                        final ReadLikelihoods<SVGenotypingContext.Allele> insertSizeLikelihoods = calculateInsertSizeLikelihoods(context, realignmentScoreArguments, scoreTable, insertSizeDistribution);
                        final ReadLikelihoods<SVGenotypingContext.Allele> discordantOrientationLikelihoods = calculateDiscordantOrientationLikelihoods(context, realignmentScoreArguments, scoreTable);
                        final ReadLikelihoods<SVGenotypingContext.Allele> totalLikelihoods = ReadLikelihoods.sum(splitsReadlikelihoods, insertSizeLikelihoods, discordantOrientationLikelihoods);
                        if (genotypeCalculator.getRelativeAlleleFrequency() != null) {
                            outputBuilder.attribute(GATKSVVCFConstants.EXPECTED_RELATIVE_ALLELE_FREQUENCIES, genotypeCalculator.getRelativeAlleleFrequency());
                        }
                        final Genotype genotype = composeGenotype(context, totalLikelihoods, genotypeCalculator, splitsReadlikelihoods, discordantOrientationLikelihoods, insertSizeLikelihoods, realignmentScoreArguments, informativePhredLikelihoodDifferenceThreshold, emitStratifiedAlleleDepths, emitStratifiedLikelihoods);
                        outputBuilder.genotypes(genotype);
                        if (outputAlignmentHeader == null) {
                            return Call.of(outputBuilder.make());
                        } else {
                            final List<SAMRecord> outputAlignemntRecords = new ArrayList<>();
                            for (final SVHaplotype haplotype : haplotypes) {

                            }
                            return Call.of(outputBuilder.make());
                        }
                    }).iterator();
        });
    }

    private static Genotype composeGenotype(final SVGenotypingContext context,
                                            final ReadLikelihoods<SVGenotypingContext.Allele> totalLikelihoods,
                                            final GenotypeLikelihoodCalculator genotypeCalculator,
                                            final ReadLikelihoods<SVGenotypingContext.Allele> splitsReadlikelihoods,
                                            final ReadLikelihoods<SVGenotypingContext.Allele> discordantOrientationLikelihoods,
                                            final ReadLikelihoods<SVGenotypingContext.Allele> insertSizeLikelihoods,
                                            final RealignmentScoreParameters penalties,
                                            final double informativePhredLikelihoodDifferenceThreshold,
                                            final boolean emitStratifiedAlleleDepths,
                                            final boolean emitStratifiedLikelihoods) {
        totalLikelihoods.removeUniformativeReads(0.0);
        totalLikelihoods.normalizeLikelihoods(true, -0.1 * penalties.maximumLikelihoodDifferencePerTemplate);
        final GenotypeLikelihoods likelihoods = genotypeCalculator.genotypeLikelihoods(totalLikelihoods.sampleMatrix(0));
        final int pl[] = likelihoods.getAsPLs();
        final int ad[] = calculateAD(totalLikelihoods, informativePhredLikelihoodDifferenceThreshold);
        final int gq = GATKVariantContextUtils.calculateGQFromPLs(pl);
        final List<Allele> genotypeAlleles = genotypeAlleleles(context, likelihoods, gq);
        final Map<String, Object> stratifiedAttributes = composeAdditionalGenotypeAttributes(emitStratifiedAlleleDepths, emitStratifiedLikelihoods, informativePhredLikelihoodDifferenceThreshold, genotypeCalculator, splitsReadlikelihoods, insertSizeLikelihoods, discordantOrientationLikelihoods);
        return new GenotypeBuilder().name(totalLikelihoods.samples().get(0))
                .PL(pl)
                .GQ(gq)
                .AD(ad)
                .DP((int) MathUtils.sum(ad))
                .attributes(stratifiedAttributes)
                .alleles(genotypeAlleles).make();
    }

    private static Map<String, Object> composeAdditionalGenotypeAttributes(boolean emitStratifiedAlleleDepths, boolean emitStratifiedLikelihoods, double informativePhredLikelihoodDifferenceThreshold, GenotypeLikelihoodCalculator genotypeCalculator, ReadLikelihoods<SVGenotypingContext.Allele> splitsReadlikelihoods, ReadLikelihoods<SVGenotypingContext.Allele> insertSizeLikelihoods, ReadLikelihoods<SVGenotypingContext.Allele> discordantOrientationLikelihoods) {
        final Map<String, Object> stratifiedAttributes = new LinkedHashMap<>((emitStratifiedAlleleDepths ? 3 : 0) + (emitStratifiedLikelihoods ? 3 : 0));
        if (emitStratifiedAlleleDepths) {
            stratifiedAttributes.put(GATKSVVCFConstants.TEMPLATE_MAPPING_LIKELIHOODS, calculateAD(splitsReadlikelihoods, informativePhredLikelihoodDifferenceThreshold));
            stratifiedAttributes.put(GATKSVVCFConstants.INSERT_SIZE_LIKELIHOODS, calculateAD(insertSizeLikelihoods, informativePhredLikelihoodDifferenceThreshold));
            stratifiedAttributes.put(GATKSVVCFConstants.DISCORDANT_PAIR_ORIENTATION_ALLELE_DEPTH, calculateAD(discordantOrientationLikelihoods, informativePhredLikelihoodDifferenceThreshold));
        }
        if (emitStratifiedLikelihoods) {
            stratifiedAttributes.put(GATKSVVCFConstants.TEMPLATE_MAPPING_LIKELIHOODS,
                    genotypeCalculator.genotypeLikelihoods(splitsReadlikelihoods.sampleMatrix(0)).getAsPLs());
            stratifiedAttributes.put(GATKSVVCFConstants.INSERT_SIZE_LIKELIHOODS,
                    genotypeCalculator.genotypeLikelihoods(insertSizeLikelihoods.sampleMatrix(0)).getAsPLs());
            stratifiedAttributes.put(GATKSVVCFConstants.DISCORDANT_PAIR_ORIENTATION_ALLELE_DEPTH,
                    genotypeCalculator.genotypeLikelihoods(discordantOrientationLikelihoods.sampleMatrix(0)).getAsPLs());
        }
        return stratifiedAttributes;
    }

    private static List<Allele> genotypeAlleleles(final SVGenotypingContext context, final GenotypeLikelihoods likelihoods, final int gq) {
        final int bestGenotypeIndex = MathUtils.maxElementIndex(likelihoods.getAsVector());
        return gq == 0
                                    ? Arrays.asList(Allele.NO_CALL, Allele.NO_CALL)
                                    : (bestGenotypeIndex == 0
                                    ? Collections.nCopies(2, context.refAllele)
                                    : ((bestGenotypeIndex == 1)
                                    ? Arrays.asList(context.refAllele, context.altAllele)
                                    : Collections.nCopies(2, context.altAllele)));
    }

    private static int[] calculateAD(final ReadLikelihoods<SVGenotypingContext.Allele> likelihoods, final double informativePhredLikelihoodDifferenceThreshold) {
        final int[] result = new int[likelihoods.numberOfAlleles()];
        final LikelihoodMatrix<SVGenotypingContext.Allele> matrix = likelihoods.sampleMatrix(0);
        final int numberOfTemplates = matrix.numberOfReads();
        final int numberOfAlleles = matrix.numberOfAlleles();
        final double log10Threshold = informativePhredLikelihoodDifferenceThreshold * -.1;
        for (int i = 0; i < numberOfTemplates; i++) {
            double bestLk = matrix.get(0, i);
            int bestLkIndex = 0;
            double secondBestLk = Double.NEGATIVE_INFINITY;
            for (int j = 1; j < numberOfAlleles; j++) {
                final double lk = matrix.get(j, i);
                if (lk > bestLk) {
                    secondBestLk = bestLk;
                    bestLk = lk;
                    bestLkIndex = j;
                } else if (lk > secondBestLk) {
                    secondBestLk = lk;
                }
            }
            if (bestLk - secondBestLk >= log10Threshold) {
                result[bestLkIndex]++;
            }
        }
        return result;
    }

    /**
     * Adjusts the expected relative frequency of alleles in the heterozygous genotype in the likelihood calculator.
     */
    private static void adjustLikelihoodCalculatorAlleleFrequencies(final SVGenotypingContext context, final InsertSizeDistribution insertSizeDistribution, final GenotypeLikelihoodCalculator genotypeCalculator) {
        if (context.variant.isInsertion()) {
            final int length = context.variant.getStructuralVariantLength();
            final double insertAverageSize = insertSizeDistribution.average();
            genotypeCalculator.setRelativeAlleleFrequency(new double[]{insertAverageSize, Math.min(2 * insertAverageSize, insertAverageSize + length)});
        } else if (context.variant.isDeletion()) {
            final double insertAverageSize = insertSizeDistribution.average();
            final int length = context.variant.getStructuralVariantLength();
            genotypeCalculator.setRelativeAlleleFrequency(new double[]{Math.min(insertAverageSize * 2, insertAverageSize + length), insertAverageSize});
        } else {
            genotypeCalculator.setRelativeAlleleFrequency(null); //reset to default uniform.
        }
    }

    private static void setMissingAlignmentScores(final RealignmentScoreParameters penalties, List<Template> templates, TemplateMappingTable scoreTable) {
        scoreTable.calculateBestMappingScores();
        for (int t = 0; t < templates.size(); t++) {
            final double worstFirstAlignmentScore = scoreTable.getWorstAlignmentScore(t, 0);
            final double firstMissingAlignmentScore = worstFirstAlignmentScore - 0.1 * penalties.unmappedFragmentPenalty;
            scoreTable.applyMissingAlignmentScore(t, 0, firstMissingAlignmentScore);
            final double worstSecondAlignmentScore = scoreTable.getWorstAlignmentScore(t, 1);
            final double secondMissingAlignmentScore = worstSecondAlignmentScore - 0.1 * penalties.unmappedFragmentPenalty;
            scoreTable.applyMissingAlignmentScore(t, 1, secondMissingAlignmentScore);
        }
    }

    private static void setMappingQualityToZeroForFragmentsThatMapDifferentlyOnFullReference(final SVGenotypingContext context, final TemplateMappingTable scoreTable) {
        for (int t = 0; t < context.numberOfTemplates; t++) {
            for (int f = 0; f < 2; f++) {
                final List<AlignmentInterval> newRefMapping = f == 0
                        ? scoreTable.getMappingInfo(context.refHaplotypeIndex, t).firstAlignmentIntervals
                        : scoreTable.getMappingInfo(context.refHaplotypeIndex, t).secondAlignmentIntervals;
                if (newRefMapping == null || newRefMapping.isEmpty()) {
                    continue;
                }
                final Template.Fragment fragment = context.templates.get(t).fragments().get(f);
                final List<AlignmentInterval> oldRefMapping = fragment.alignmentIntervals();
                if (oldRefMapping == null || oldRefMapping.isEmpty()) {
                    continue;
                }
                if (newRefMapping.size() != oldRefMapping.size()) {
                    context.templates.get(t).fragments().get(f).setMappingQuality(0);
                } else {
                    for (final AlignmentInterval newInterval : newRefMapping) {
                        if (oldRefMapping.stream().noneMatch(old ->
                                old.startInAssembledContig == newInterval.startInAssembledContig
                                        && old.endInAssembledContig == newInterval.endInAssembledContig
                                        && CigarUtils.equals(old.cigarAlong5to3DirectionOfContig, newInterval.cigarAlong5to3DirectionOfContig)
                                        && old.referenceSpan.getContig().equals(context.altHaplotype.getReferenceSpan().getContig())
                                        && old.referenceSpan.getStart() == context.refHaplotype.getReferenceSpan().getStart() + newInterval.referenceSpan.getStart() - 1)) {
                            context.templates.get(t).fragments().get(f).setMappingQuality(0);
                            break;
                        }
                    }
                }
            }
        }
    }

    private static TemplateMappingTable remapTemplatesOnHaplotypes(final SVGenotypingContext context, SerializableBiFunction<String, byte[], File> imageCreator, RealignmentScoreParameters realignmentScoreArguments, Map<String, File> imagesByName) {
        final List<byte[]> sequences = context.templates.stream()
                .flatMap(t -> t.fragments().stream())
                .map(Template.Fragment::bases)
                .collect(Collectors.toList());
        final TemplateMappingTable scoreTable =
                new TemplateMappingTable(context.templates, context.haplotypes);
        for (int h = 0; h < context.numberOfHaplotypes; h++) {
            final SVHaplotype haplotype = context.haplotypes.get(h);

            final boolean isContig = haplotype.isNeitherReferenceNorAlternative();

            final String imageName = isContig
                    ? haplotype.getName()
                    : context.variant.getUniqueID() + "/" + haplotype.getName();
            final File imageFile =
                    imagesByName.computeIfAbsent(imageName, (in) -> imageCreator.apply(in, haplotype.getBases()));
            final BwaMemIndex index = BwaMemIndexCache.getInstance(imageFile.toString());
            final BwaMemAligner aligner = new BwaMemAligner(index);
            aligner.alignPairs();
            aligner.dontInferPairEndStats();
            // Interestingly it turns out that either allow bwa to infer the insert size or provide yoursel
            // results in reducced accuracy!!! I think that this and indication that is best not to try t
            // recover with SW unmapped mates as perhaps their alignment will always be relative poor and just "mud the already muddy waters"
            //aligner.setProperPairEndStats(new BwaMemPairEndStats(insertSizeDistribution.average(),
            //        insertSizeDistribution.stddev(), insertSizeDistribution.quantile(0.01),
            //        insertSizeDistribution.quantile(0.99)));
            final List<List<BwaMemAlignment>> alignments = aligner.alignSeqs(sequences);
            final IntFunction<String> haplotypeName = i -> i == 0 ? haplotype.getName() : null;
            for (int i = 0; i < context.numberOfTemplates; i++) {
                final Template template = context.templates.get(i);
                final List<BwaMemAlignment> firstAlignment = alignments.get(i * 2);
                final List<BwaMemAlignment> secondAlignment = alignments.get(i * 2 + 1);
                final List<AlignmentInterval> firstIntervals = BwaMemAlignmentUtils.toAlignmentIntervals(firstAlignment, haplotypeName, template.fragments().get(0).length());
                final List<AlignmentInterval> secondIntervals = BwaMemAlignmentUtils.toAlignmentIntervals(secondAlignment, haplotypeName, template.fragments().get(1).length());

                final TemplateMapping mappingInformation = TemplateMapping.fromAlignments(realignmentScoreArguments, haplotype,
                        template.fragments().get(0).bases(), firstIntervals,
                        template.fragments().get(1).bases(), secondIntervals);
                scoreTable.setMapping(h, i, mappingInformation);
            }
        }
        return scoreTable;
    }

    private static ReadLikelihoods<SVGenotypingContext.Allele> calculateSplitReadLikelihoods(final SVGenotypingContext context, final RealignmentScoreParameters realignmentScoreArguments,
                                                                                             final TemplateMappingTable scoreTable) {

        final ReadLikelihoods<SVGenotypingContext.Allele> likelihoods = context.newLikelihoods();
        final ReadLikelihoods<SVGenotypingContext.Allele> likelihoodsFirst = context.newLikelihoods();
        final ReadLikelihoods<SVGenotypingContext.Allele> likelihoodsSecond = context.newLikelihoods();

        final LikelihoodMatrix<SVGenotypingContext.Allele> sampleLikelihoods = likelihoods.sampleMatrix(0);
        final LikelihoodMatrix<SVGenotypingContext.Allele> sampleLikelihoodsFirst = likelihoodsFirst.sampleMatrix(0);
        final LikelihoodMatrix<SVGenotypingContext.Allele> sampleLikelihoodsSecond = likelihoodsSecond.sampleMatrix(0);

        sampleLikelihoods.fill(Double.NEGATIVE_INFINITY);
        sampleLikelihoods.fill(Double.NEGATIVE_INFINITY);
        final List<SVContig> contigs = context.haplotypes.stream().filter(SVHaplotype::isNeitherReferenceNorAlternative).map(SVContig.class::cast)
                .collect(Collectors.toList());
        for (int t = 0; t < context.numberOfTemplates; t++) {
            sampleLikelihoodsFirst.set(context.refAlleleIndex, t,
                    scoreTable.getMappingInfo(context.refHaplotypeIndex, t).firstAlignmentScore.orElse(0));
            sampleLikelihoodsSecond.set(context.refAlleleIndex, t,
                    scoreTable.getMappingInfo(context.refHaplotypeIndex, t).secondAlignmentScore.orElse(0));
            sampleLikelihoodsFirst.set(context.altAlleleIndex, t,
                    scoreTable.getMappingInfo(context.altHaplotypeIndex, t).firstAlignmentScore.orElse(0));
            sampleLikelihoodsSecond.set(context.altAlleleIndex, t,
                    scoreTable.getMappingInfo(context.altHaplotypeIndex, t).secondAlignmentScore.orElse(0));
        }

        final Set<String> altContigNames = new HashSet<>(context.variant.getSupportingContigIds());

        for (final SVContig contig : contigs) {
            final int mappingInfoIndex = context.haplotypes.indexOf(contig);
            double haplotypeAltScore = RealignmentScore.calculate(realignmentScoreArguments, context.haplotypes.get(context.altHaplotypeIndex).getBases(), contig.getBases(), contig.getAlternativeAlignment()).getLog10Prob();
            double haplotypeRefScore = RealignmentScore.calculate(realignmentScoreArguments, context.haplotypes.get(context.refHaplotypeIndex).getBases(), contig.getBases(), contig.getReferenceAlignment()).getLog10Prob();
            if (altContigNames.contains(contig.getName())) {
                haplotypeAltScore = 0;
                haplotypeRefScore = -60;
            }
            final double maxMQ = contig.getMinimumMappingQuality();
            final double base = Math.max(haplotypeAltScore, haplotypeRefScore);
            haplotypeAltScore -= base;
            haplotypeRefScore -= base;
            // we cap the difference of scores by the contig mapping quality.
            // so that contigs that could map in several places in the genome has less weight when assigning
            // genotypes.
            if (haplotypeAltScore < haplotypeRefScore) {
                haplotypeAltScore = Math.max(haplotypeAltScore, haplotypeRefScore - 0.1 * maxMQ);
            } else {
                haplotypeRefScore = Math.max(haplotypeRefScore, haplotypeAltScore - 0.1 * maxMQ);
            }
            if (haplotypeRefScore == haplotypeAltScore) {
                haplotypeAltScore = haplotypeRefScore = Double.NEGATIVE_INFINITY;
            }

            // for each template we apply the scores thru the haplotype/contig `c`. We reduce/marginalize the likelihood
            // to take the maximum across all haplotype/contigs for that template.
            for (int t = 0; t < context.numberOfTemplates; t++) {
                final boolean noAlignment = !scoreTable.getMappingInfo(mappingInfoIndex, t).firstAlignmentScore.isPresent()
                        && !scoreTable.getMappingInfo(mappingInfoIndex, t).secondAlignmentScore.isPresent();
                if (noAlignment) continue;
                final double firstMappingScore = scoreTable.getMappingInfo(mappingInfoIndex, t).firstAlignmentScore.orElse(Double.NEGATIVE_INFINITY);
                final double secondMappingScore = scoreTable.getMappingInfo(mappingInfoIndex, t).secondAlignmentScore.orElse(Double.NEGATIVE_INFINITY);
                sampleLikelihoodsFirst.set(context.refAlleleIndex, t,
                        Math.max(firstMappingScore + haplotypeRefScore, sampleLikelihoodsFirst.get(context.refAlleleIndex, t)));
                sampleLikelihoodsFirst.set(context.altAlleleIndex, t,
                        Math.max(firstMappingScore + haplotypeAltScore, sampleLikelihoodsFirst.get(context.altAlleleIndex, t)));
                sampleLikelihoodsSecond.set(context.refAlleleIndex, t,
                        Math.max(secondMappingScore + haplotypeRefScore, sampleLikelihoodsSecond.get(context.refAlleleIndex, t)));
                sampleLikelihoodsSecond.set(context.altAlleleIndex, t,
                        Math.max(secondMappingScore + haplotypeAltScore, sampleLikelihoodsSecond.get(context.altAlleleIndex, t)));
            }
        }
        // we cap the likelihood difference for each allele haplotypes for each fragment in each template by th
        // mapping quality of the fragment so that those reads that may map to other locations in the genome count less
        // towards genotyping.
        for (int t = 0; t < context.numberOfTemplates; t++) {
            for (int f = 0; f < 2; f++) {
                final int maxMq = context.templates.get(t).fragments().get(f).getMappingQuality();
                final LikelihoodMatrix<SVGenotypingContext.Allele> matrix = f == 0 ? sampleLikelihoodsFirst : sampleLikelihoodsSecond;
                final double base = Math.max(matrix.get(context.refAlleleIndex, t), matrix.get(context.altAlleleIndex, t));
                final int maxIndex = matrix.get(context.refAlleleIndex, t) == base ? context.refAlleleIndex : context.altAlleleIndex;
                final int minIndex = maxIndex == context.refAlleleIndex ? context.altAlleleIndex : context.refAlleleIndex;
                matrix.set(minIndex, t, Math.max(matrix.get(maxIndex, t) - 0.1 * maxMq, matrix.get(minIndex, t)));
            }
        }


        for (int j = 0; j < sampleLikelihoods.numberOfReads(); j++) {
            final boolean considerFirstFragment = scoreTable.getMappingInfo(context.refHaplotypeIndex, j).crossesBreakPoint(context.refBreakPoints)
                    || scoreTable.getMappingInfo(context.altHaplotypeIndex, j).crossesBreakPoint(context.altBreakPoints);
            final boolean considerSecondFragment = scoreTable.getMappingInfo(context.refHaplotypeIndex, j).crossesBreakPoint(context.refBreakPoints)
                    || scoreTable.getMappingInfo(context.altHaplotypeIndex, j).crossesBreakPoint(context.altBreakPoints);

            //TODO  the following adjustment seem to increase accuracy but the maths are a bit sketchy so for now I
            //include it.
            // The idea is to penalize poorly modeled reads... those that allele haplotype likelihood difference is not
            // much larger than the magnitude of the likelihood of the best allele likelihood.
            // so for example a template fragment has lks Phred 3000 3100, there is 100 diff in Lk but is very small
            // when compare to how unlikely the read is even with the best haplotype (3000) so is 100 even relevant?
            // The code simply substract 3000 to those 100 (min 0) so in the end such a read to be of any value for Lk
            // calculation the worst allele lk would need to be at least 6000.
            //
            // at first may seem a reasonable thing to do but it it unclear how to weight down these reads... just take
            // the best lk seems arbitrary.
            //
            // This might be resolved differently by ignoring short variant differences so that the Lk is totally
            // defined by real SV variation and so the 3000 becomes closer to 0. Or that in general the 100 is actual
            // real SV differences rather that differences due to chance in the distribution of short variants between
            // contigs.
            //if (sampleLikelihoodsFirst == sampleLikelihoodsSecond) debug00(sampleLikelihoodsFirst, sampleLikelihoodsSecond);
            for (int k = 0; k < 2; k++) {
                final LikelihoodMatrix<SVGenotypingContext.Allele> matrix = k == 0 ? sampleLikelihoodsFirst : sampleLikelihoodsSecond;
                final double base = Math.max(matrix.get(context.refAlleleIndex, j), matrix.get(context.altAlleleIndex, j));
                final int maxIndex = matrix.get(context.refAlleleIndex, j) == base ? context.refAlleleIndex : context.altAlleleIndex;
                final int minIndex = maxIndex == context.refAlleleIndex ? context.altAlleleIndex : context.refAlleleIndex;
                matrix.set(minIndex, j, Math.min(matrix.get(maxIndex, j), matrix.get(minIndex, j) - scoreTable.getBestAlignmentScore(j, k)));
            }
            for (int i = 0; i < sampleLikelihoods.numberOfAlleles(); i++) {
                sampleLikelihoods.set(i, j, (considerFirstFragment ? sampleLikelihoodsFirst.get(i, j) : 0) +
                        ((considerSecondFragment) ? sampleLikelihoodsSecond.get(i, j) : 0));

            }
        }
        return likelihoods;
    }

    private static ReadLikelihoods<SVGenotypingContext.Allele> calculateDiscordantOrientationLikelihoods(final SVGenotypingContext context,
                                                                                                         final RealignmentScoreParameters penalties,
                                                                                                         final TemplateMappingTable scoreTable) {
        final ReadLikelihoods<SVGenotypingContext.Allele> result = context.newLikelihoods();
        final LikelihoodMatrix<SVGenotypingContext.Allele> matrix = result.sampleMatrix(0);
        for (int t = 0; t < context.numberOfTemplates; t++) {
            final TemplateMapping referenceMappingInfo = scoreTable.getMappingInfo(context.refHaplotypeIndex, t);
            final TemplateMapping alternativeMappingInfo = scoreTable.getMappingInfo(context.altHaplotypeIndex, t);
            if (!referenceMappingInfo.pairOrientation.isDefined()) continue;
            if (!alternativeMappingInfo.pairOrientation.isDefined()) continue;
            if (referenceMappingInfo.pairOrientation.isProper() == alternativeMappingInfo.pairOrientation.isProper())
                continue;
            if (referenceMappingInfo.pairOrientation.isProper()) { // && referenceMappingInfo.crossesBreakPoint(context.refBreakPoints)) {
                matrix.set(context.altAlleleIndex, t, penalties.improperPairPenalty * -.1);
            } else { // this must be true: alternativeMappingInfo.pairOrientation.isProper()
                matrix.set(context.refAlleleIndex, t, penalties.improperPairPenalty * -.1);
            }
        }
        return result;
    }

    private static ReadLikelihoods<SVGenotypingContext.Allele> calculateInsertSizeLikelihoods(final SVGenotypingContext context,
                                                                                              final RealignmentScoreParameters penalties,
                                                                                              final TemplateMappingTable scoreTable,
                                                                                              final InsertSizeDistribution dist) {
        final ReadLikelihoods<SVGenotypingContext.Allele> result = context.newLikelihoods();
        final LikelihoodMatrix<SVGenotypingContext.Allele> matrix = result.sampleMatrix(0);
        final double inverseOfLog10 = 1.0 / MathUtil.LOG_10_MATH.getLog_of_base();
        for (int t = 0; t < context.numberOfTemplates; t++) {
            final TemplateMapping referenceMappingInfo = scoreTable.getMappingInfo(context.refHaplotypeIndex, t);
            final TemplateMapping alternativeMappingInfo = scoreTable.getMappingInfo(context.altHaplotypeIndex, t);
            if (!referenceMappingInfo.pairOrientation.isProper()) continue;
            if (!alternativeMappingInfo.pairOrientation.isProper()) continue;
            final boolean accrossBreakPointsOnRef = referenceMappingInfo.crossesBreakPoint(context.refBreakPoints);
            final boolean accrossBreakPointsOnAlt = alternativeMappingInfo.crossesBreakPoint(context.altBreakPoints);
            if (accrossBreakPointsOnAlt || accrossBreakPointsOnRef) {
                @SuppressWarnings("ConstantConditions")
                final double refInsertSizeLk = dist.logProbability(referenceMappingInfo.insertSize.getAsInt()) * inverseOfLog10;
                @SuppressWarnings("ConstantConditions")
                final double altInsertSizeLk = dist.logProbability(alternativeMappingInfo.insertSize.getAsInt()) * inverseOfLog10;
                final double best = Math.max(refInsertSizeLk, altInsertSizeLk);
                final double worst = best + penalties.improperPairPenalty * -.1;
                matrix.set(context.refAlleleIndex, t, Math.max(worst, refInsertSizeLk));
                matrix.set(context.altAlleleIndex, t, Math.max(worst, altInsertSizeLk));
            }
        }
        return result;
    }

    private static boolean structuralVariantAlleleIsSupported(final SVContext ctx) {
        switch (ctx.getStructuralVariantType()) {
            case INS:
            case DEL:
            case DUP:
            case INV:
                return true;
            default:
                return false;
        }
    }


    private void tearDown(@SuppressWarnings("unused") final JavaSparkContext ctx) {
        variantsSource = null;
    }
}
