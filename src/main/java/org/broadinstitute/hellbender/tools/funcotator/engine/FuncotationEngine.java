package org.broadinstitute.hellbender.tools.funcotator.engine;

import htsjdk.tribble.Feature;
import htsjdk.variant.variantcontext.VariantContext;
import org.broadinstitute.hellbender.engine.FeatureContext;
import org.broadinstitute.hellbender.engine.FeatureInput;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.tools.funcotator.*;
import org.broadinstitute.hellbender.tools.funcotator.dataSources.gencode.GencodeFuncotation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FuncotationEngine {

    /**
     * Creates an annotation on the given {@code variant} or enqueues it to be processed during a later call to this method.
     *
     * @param variant          {@link VariantContext} to annotate.
     * @param referenceContext {@link ReferenceContext} corresponding to the given {@code variant}.
     * @param featureContext   {@link FeatureContext} corresponding to the given {@code variant}.
     */
    public FuncotationMap enqueueAndHandleVariant(final VariantContext variant, final ReferenceContext referenceContext, final FeatureContext featureContext) {
        // Get our manually-specified feature inputs:

        final Map<String, List<Feature>> featureSourceMap = new HashMap<>();

        for ( final FeatureInput<? extends Feature> featureInput : manualLocatableFeatureInputs ) {
            @SuppressWarnings("unchecked")
            final List<Feature> featureList = (List<Feature>)featureContext.getValues(featureInput);
            featureSourceMap.put( featureInput.getName(), featureList );
        }

        //==============================================================================================================
        // First create only the transcript (Gencode) funcotations:

        if (retrieveGencodeFuncotationFactoryStream().count() > 1) {
            logger.warn("Attempting to annotate with more than one GENCODE datasource.  If these have overlapping transcript IDs, errors may occur.");
        }

        final List<GencodeFuncotation> transcriptFuncotations = retrieveGencodeFuncotationFactoryStream()
                .map(gf -> gf.createFuncotations(variant, referenceContext, featureSourceMap))
                .flatMap(List::stream)
                .map(gf -> (GencodeFuncotation) gf).collect(Collectors.toList());

        //==============================================================================================================
        // Create the funcotations for non-Gencode data sources:

        // Create a place to keep our funcotations:
        final FuncotationMap funcotationMap = FuncotationMap.createFromGencodeFuncotations(transcriptFuncotations);

        // Perform the rest of the annotation.  Note that this code manually excludes the Gencode Funcotations.
        for (final DataSourceFuncotationFactory funcotationFactory : dataSourceFactories ) {

            // Note that this guarantees that we do not add GencodeFuncotations a second time.
            if (!funcotationFactory.getType().equals(FuncotatorArgumentDefinitions.DataSourceType.GENCODE)) {
                final List<String> txIds = funcotationMap.getTranscriptList();

                for (final String txId: txIds) {
                    funcotationMap.add(txId, funcotationFactory.createFuncotations(variant, referenceContext, featureSourceMap, funcotationMap.getGencodeFuncotations(txId)));
                }
            }
        }

        //==============================================================================================================
        // Create the funcotations for the input and add to all txID mappings.

        final List<String> txIds = funcotationMap.getTranscriptList();

        for (final String txId: txIds) {
            funcotationMap.add(txId, FuncotatorUtils.createFuncotations(variant, inputMetadata, FuncotatorConstants.DATASOURCE_NAME_FOR_INPUT_VCFS));
        }

        return funcotationMap;
    }
}
