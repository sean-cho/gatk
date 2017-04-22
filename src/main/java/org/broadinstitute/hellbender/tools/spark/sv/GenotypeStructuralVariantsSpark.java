package org.broadinstitute.hellbender.tools.spark.sv;

import htsjdk.variant.variantcontext.VariantContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.ArgumentCollection;
import org.broadinstitute.barclay.argparser.CommandLineException;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.argumentcollections.RequiredVariantInputArgumentCollection;
import org.broadinstitute.hellbender.cmdline.programgroups.StructuralVariationSparkProgramGroup;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.engine.spark.datasources.VariantsSparkSource;
import org.broadinstitute.hellbender.utils.variant.GATKVariant;

import java.io.File;
import java.util.stream.Collectors;

/**
 * Created by valentin on 4/20/17.
 */
@CommandLineProgramProperties(summary = "genotype SV variant call files",
        oneLineSummary = "genotype SV variant call files",
        programGroup = StructuralVariationSparkProgramGroup.class)
public class GenotypeStructuralVariantsSpark extends GATKSparkTool {

    @ArgumentCollection
    private RequiredVariantInputArgumentCollection variantArguments;

    @Argument(doc = "output VCF file",
            shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
            optional = true)
    private File outputFile = null;

    private VariantsSparkSource variantsSource;


    private void setUp(final JavaSparkContext ctx) {
        if (!outputFile.getParentFile().isDirectory()) {
            throw new CommandLineException.BadArgumentValue(StandardArgumentDefinitions.OUTPUT_SHORT_NAME, "the output file location is not a directory:");
        } else if (outputFile.exists() && !outputFile.isFile()) {
            throw new CommandLineException.BadArgumentValue(StandardArgumentDefinitions.OUTPUT_SHORT_NAME, "the output file makes reference to something that is not a file");
        }
        variantsSource = new VariantsSparkSource(ctx);
    }

    @Override
    protected void runTool(final JavaSparkContext ctx) {
        setUp(ctx);
        final JavaRDD<GATKVariant> variants = variantsSource.getParallelVariants(variantArguments.variantFiles.stream().map(vf -> vf.getFeaturePath()).collect(Collectors.toList()), getIntervals());
        final JavaRDD<GATKVariant> outputVariants = processVariants(variants, ctx);
        SVVCFWriter.writeVCF(getAuthenticatedGCSOptions(), outputFile.getParent(), outputFile.getName(), referenceArguments.getReferenceFile().getAbsolutePath(), outputVariants.map(gatkv -> (VariantContext) gatkv), logger);
        tearDown(ctx);
    }

    private JavaRDD<GATKVariant> processVariants(final JavaRDD<GATKVariant> variants, final JavaSparkContext ctx) {
        return variants;
    }

    private void tearDown(final JavaSparkContext ctx) {
        variantsSource = null;
    }
}
