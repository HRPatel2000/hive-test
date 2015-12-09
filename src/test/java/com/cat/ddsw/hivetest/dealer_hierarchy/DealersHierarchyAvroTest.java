package com.cat.ddsw.hivetest.dealer_hierarchy;

import com.cat.ddsw.hivetest.ddl_repair.DDLRepairUtil;
import com.cat.ddsw.hivetest.util.HiveFileUtil;
import com.cat.ddsw.hivetest.util.HiveProperties;
import com.cat.ddsw.hivetest.util.Sha1Hex;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * Create Delta Table - Dealer's Hierarchy : Reference implementation.
 * <p/>
 * All HiveRunner tests should run with the StandaloneHiveRunner
 */
@RunWith(StandaloneHiveRunner.class)
public class DealersHierarchyAvroTest {

    //Local HDFS Path where Data and Schema are stored
    private final String hdfsSource = "${hiveconf:hadoop.tmp.dir}/dealer_hierarchy/temp_data";

    //Local Resources for Data and Schema to be loaded into Local HDFS
    private String dataFromString = "dealer_021205|021205|DMS|2.0|2008-12-18T12:12:12|2008-12-18T12:12:12|A001|C|A|01|AA|GENIUS_SPECTRUM|01|L100|L100|CAT|001|2010-10-30T12:12:12|2010-10-30T12:12:12|HPATEL|2010-10-30T12:12:12|2010-10-30T12:12:12|001|AAAA";
    private File tempDataFile = new File(HiveProperties.getInstance().getProperty("hive.temp.data.file"));

    //Local Setup Files for Table Hive DB Creation
    File setupHiveUDFFile = new File(HiveProperties.getInstance().getProperty("hive.udf.file"));
    File setupCoreTableFile = new File(HiveProperties.getInstance().getProperty("hive.core.avro.table.file"));
    File setupTempDataTableFile = new File(HiveProperties.getInstance().getProperty("hive.temp.table.file"));

    /**
     * Define the script files under test. The files will be loaded in the given order.
     * <p/>
     * The HiveRunner instantiate and inject the HiveShell
     */
    //****IMPORTANT****
    //HiveShell AutoStart must be False here
    @HiveSQL(files = {
    }, encoding = "UTF-8", autoStart = false)
    private HiveShell hiveShell;

    @Before
    public void initialize() {

        hiveShell.addResource(hdfsSource + "/dealer_code=AAAA/temp_data_from_string.csv", dataFromString);
        hiveShell.addResource(hdfsSource + "/dealer_code=ZZZZ/temp_data_from_file.csv", tempDataFile);

        //****IMPORTANT****
        //Sequence/Order of below Setup HQLs matter, so do not alter
        hiveShell.addSetupScripts(
                Charset.forName("UTF-8"),
                setupHiveUDFFile,
                setupTempDataTableFile,
                setupCoreTableFile
        );

        //HiveShell must be started
        hiveShell.start();

        //HiveShell must be started and Paritions must be created on Hive Tables before Repairing Partitions
        DDLRepairUtil.repairTables(
                hiveShell,
                "dealer_hierarchy_core",
                "dealer_hierarchy_temp_data");

        String insertDataIntoCoreTable = null;

        try {
            insertDataIntoCoreTable = HiveFileUtil.readFile(HiveProperties.getInstance().getProperty("hive.core.avro.table.data.file"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        hiveShell.execute(insertDataIntoCoreTable);
    }

    @Test
    public void testSetHiveQueueName() {
        String expected = "ddsw";
        String actual = hiveShell.getHiveConf().get("mapreduce.job.queuename");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSetHiveExecDropIgnoreNonExistent() {
        String expected = "true";
        String actual = hiveShell.getHiveConf().get("hive.exec.drop.ignorenonexistent");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testDatabaseCreated() {
        List<String> expected = Arrays.asList("ddsw_dev", "default");
        List<String> actual = hiveShell.executeQuery("show databases");

        Collections.sort(expected);
        Collections.sort(actual);

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testTablesCreated() {
        List<String> expected = Arrays.asList("dealer_hierarchy_core", "dealer_hierarchy_temp_data");
        List<String> actual = hiveShell.executeQuery("show tables");

        Collections.sort(expected);
        Collections.sort(actual);

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testDataCountFromCoreTableAvroFormat() {

        List<String> expected = Arrays.asList("4");
        List<String> actual = hiveShell.executeQuery("select COUNT(*) from dealer_hierarchy_temp_data");
        Assert.assertEquals(expected, actual);

        actual = hiveShell.executeQuery("select COUNT(*) from dealer_hierarchy_core");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testPartitionDataCountFromCoreTableAvroFormat() {

        List<String> actual = hiveShell.executeQuery("select COUNT(*) from dealer_hierarchy_temp_data where dealer_code='AAAA'");
        Assert.assertEquals(Arrays.asList("1"), actual);

        actual = hiveShell.executeQuery("select COUNT(*) from dealer_hierarchy_temp_data where dealer_code='ZZZZ'");
        Assert.assertEquals(Arrays.asList("3"), actual);

        actual = hiveShell.executeQuery("select COUNT(*) from dealer_hierarchy_core where dealer_code='AAAA'");
        Assert.assertEquals(Arrays.asList("1"), actual);

        actual = hiveShell.executeQuery("select COUNT(*) from dealer_hierarchy_core where dealer_code='ZZZZ'");
        Assert.assertEquals(Arrays.asList("3"), actual);
    }

    @Test
    public void testCoreTableContentByHash() {

        String expectedHash = null;
        String actualHash = null;

        //Adjust Input to account for UDFs Date/Timestamp Formatting
        String formattedInputForPartitionAAAA = "dealer_021205|021205|DMS|2.0|2008-12-18 12:12:12.000|2008-12-18 12:12:12 +00:00|A001|C|A|01|AA|GENIUS_SPECTRUM|01|L100|L100|CAT|001|2010-10-30T12:12:12|2010-10-30T12:12:12|HPATEL|2010-10-30T12:12:12|2010-10-30T12:12:12|001|AAAA";

        System.out.println("Dealer_Hierarchy : Expected Row :-> " + formattedInputForPartitionAAAA.replace('|', '\t'));

        try {
            expectedHash = Sha1Hex.makeSHA1Hash(formattedInputForPartitionAAAA.replace('|', '\t'));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        System.out.println("Dealer_Hierarchy : Expected Hash :-> " + expectedHash);

        List<String> actual = hiveShell.executeQuery("select * from dealer_hierarchy_core where dealer_code='AAAA'");
        if(!actual.isEmpty()) {

            String actualRow = actual.get(0);
            System.out.println("Dealer_Hierarchy : Actual Row :-> " + actualRow);
            try {
                actualHash = Sha1Hex.makeSHA1Hash(actualRow);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            System.out.println("Dealer_Hierarchy : Actual Hash :-> " + actualHash);
        }

        Assert.assertEquals(expectedHash, actualHash);
    }

    @Test
    public void testCoreTablePartitionCreated() {
        List<String> expected = Arrays.asList("dealer_code=AAAA", "dealer_code=ZZZZ");
        List<String> actual = hiveShell.executeQuery("SHOW PARTITIONS dealer_hierarchy_temp_data");
        Assert.assertEquals(expected, actual);

        actual = hiveShell.executeQuery("SHOW PARTITIONS dealer_hierarchy_core");
        Assert.assertEquals(expected, actual);
    }
}
