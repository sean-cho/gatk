package org.broadinstitute.hellbender.tools.funcotator;

import org.apache.commons.io.FileUtils;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.BetaFeature;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.hellbender.engine.GATKTool;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.io.IOUtils;
import picard.cmdline.programgroups.VariantEvaluationProgramGroup;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;

/**
 * Tool to download the latest data sources for {@link Funcotator}.
 * Created by jonn on 8/23/18.
 */
@CommandLineProgramProperties(
        summary = "Download the Broad Institute pre-packaged data sources for the somatic or germline use case for Funcotator.",
        oneLineSummary = "Data source downloader for Funcotator.",
        programGroup = VariantEvaluationProgramGroup.class
)
@DocumentedFeature
@BetaFeature
public class FuncotatorDataSourceRetriever extends GATKTool {

    //==================================================================================================================
    // Public Static Members:

    //==================================================================================================================
    // Private Static Members:

    private static String GERMLINE_GCLOUD_DATASOURCES_BASEURL = "gs://broad-public-datasets/funcotator/funcotator_dataSources.v1.4.20180615";
    private static Path GERMLINE_GCLOUD_DATASOURCES_PATH = IOUtils.getPath(GERMLINE_GCLOUD_DATASOURCES_BASEURL + ".tar.gz");
    private static Path GERMLINE_GCLOUD_DATASOURCES_SHA256_PATH = IOUtils.getPath(GERMLINE_GCLOUD_DATASOURCES_BASEURL + ".sha256");

    //==================================================================================================================
    // Private Members:

    @Argument(fullName = "validate-integrity",
            shortName = "validate-integrity",
            doc = "Validate the integrity of the data sources after downloading them using sha256.", optional = true)
    private boolean validateIntegrity = false;

    @Argument(fullName = "somatic",
            shortName = "somatic",
            mutex = {"germline"},
            doc = "Download the latest pre-packaged datasources for somatic functional annotation.", optional = true)
    private boolean getSomaticDataSources = false;

    @Argument(fullName = "germline",
            shortName = "germline",
            mutex = {"somatic"},
            doc = "Download the latest pre-packaged datasources for germline functional annotation.", optional = true)
    private boolean getGermlineDataSources = false;

    //==================================================================================================================
    // Constructors:

    //==================================================================================================================
    // Override Methods:


    @Override
    protected void onStartup() {
        super.onStartup();

        // Make sure the user specified at least one data source to download:
        if ((!getSomaticDataSources) && (!getGermlineDataSources)) {
            throw new UserException("Must select either somatic or germline datasources.");
        }
    }

    @Override
    public void traverse() {

        // Get the correct data source:
        if ( getSomaticDataSources ) {

        }

        if ( getGermlineDataSources ) {

            // Get the germline datasources file:
            final Path germlineDataSources = downloadDatasources(GERMLINE_GCLOUD_DATASOURCES_PATH);

            // Validate the data sources if requested:
            if (validateIntegrity && !validateIntegrity(germlineDataSources, GERMLINE_GCLOUD_DATASOURCES_SHA256_PATH)) {
                throw new GATKException("Downloaded data sources were not valid!  Incorrect sha256sum encountered!");
            }
        }
    }

    //==================================================================================================================
    // Static Methods:

    //==================================================================================================================
    // Instance Methods:

    private Path downloadDatasources(final Path datasourcesPath) {

        // Get the data sources file:
        final Path outputDestination = getDataSourceFolderPath(datasourcesPath);
        try {
            java.nio.file.Files.copy(datasourcesPath, outputDestination);
        }
        catch (final IOException ex) {
            throw new GATKException("Could not copy data sources file: " + datasourcesPath.toUri().toString() + " -> " + outputDestination.toUri().toString(), ex);
        }

        return outputDestination;
    }

    private boolean validateIntegrity(final Path localDataSourcesPath, final Path remoteSha256Path) {

        // Get the SHA 256 file:
        final Path localSha256Path = getDataSourceFolderPath(remoteSha256Path);
        try {
            java.nio.file.Files.copy(remoteSha256Path, localSha256Path);
        }
        catch (final IOException ex) {
            throw new GATKException("Could not copy sha256 sum from server: " + remoteSha256Path.toUri().toString() + " -> " + localSha256Path.toUri().toString(), ex);
        }

        // Read the sha256sum into memory:
        final String expectedSha256Sum;
        try {
            expectedSha256Sum = FileUtils.readFileToString(localSha256Path.toFile(), Charset.defaultCharset());
        }
        catch ( final IOException ex ) {
            throw new GATKException("Could not read in sha256sum from file: " + localSha256Path.toUri().toString(), ex);
        }

        // Calculate the sha256sum of the data sources file:
        final String actualSha256Sum = DatatypeConverter.printHexBinary(...);

        return true;
    }

    private Path getDataSourceFolderPath(final Path dataSourcesPath) {
        return dataSourcesPath.getName(-1);
    }

    //==================================================================================================================
    // Helper Data Types:

}
