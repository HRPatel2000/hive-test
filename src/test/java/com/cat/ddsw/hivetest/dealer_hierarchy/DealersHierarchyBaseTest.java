/*
 * Copyright 2013 Klarna AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cat.ddsw.hivetest.dealer_hierarchy;

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
public class DealersHierarchyBaseTest {


    //Local HDFS Path where Data and Schema are stored
    private final String hdfsSource = "${hiveconf:hadoop.tmp.dir}/dealer_hierarchy";

    //Local Resources for Data and Schema to be loaded into Local HDFS
    private String dataFromString = "dealer_021205|021205|DMS|2.0|2008-12-18T12:12:12|2008-12-18T12:12:12|A001|C|A|01|AA|GENIUS_SPECTRUM|01|L100|L100|CAT|001|2010-10-30T12:12:12|2010-10-30T12:12:12|HPATEL|2010-10-30T12:12:12|2010-10-30T12:12:12|001|AAAA";
    private File tempDataFile = new File(HiveProperties.getInstance().getProperty("hive.temp.data.file"));

    //Local Setup Files for Table Hive DB Creation
    File setupHiveUDFFile = new File(HiveProperties.getInstance().getProperty("hive.udf.file"));
    File setupBaseTablesFile = new File(HiveProperties.getInstance().getProperty("hive.base.table.file"));
    File setupDeltaTablesFile = new File(HiveProperties.getInstance().getProperty("hive.delta.table.file"));

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

        hiveShell.addResource(hdfsSource + "/temp_data_from_string.csv", dataFromString);
        hiveShell.addResource(hdfsSource + "/temp_data_from_file.csv", tempDataFile);

        //****IMPORTANT****
        //Sequence/Order of below Setup HQLs matter, so do not alter
        hiveShell.addSetupScripts(
                Charset.forName("UTF-8"),
                setupHiveUDFFile,
                setupBaseTablesFile,
                setupDeltaTablesFile
        );

        //HiveShell must be started
        hiveShell.start();

        //****IMPORTANT****
        //CTAS Table needs to be created after the Delta Table is created and Data is loaded into it as part of the SetupScripts above
        String createDeltaCTASTable = null;

        try {
            createDeltaCTASTable = HiveFileUtil.readFile(HiveProperties.getInstance().getProperty("hive.delta.ctas.table.file"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        hiveShell.execute(createDeltaCTASTable);
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
        List<String> expected = Arrays.asList("dealer_hierarchy_delta", "dealer_hierarchy_delta_prim", "dealer_hierarchy_xmlvalidationerrors");
        List<String> actual = hiveShell.executeQuery("show tables");

        Collections.sort(expected);
        Collections.sort(actual);

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testDeltaTablePropertiesApplied() {
        //TBLPROPERTIES
        //  (
            // 'numFiles'='20',
            // 'STATS_GENERATED_VIA_STATS_TASK'='true',
            // 'totalSize'='153086'
        // )
        Assert.assertEquals("20", hiveShell.executeQuery("show tblproperties dealer_hierarchy_delta(\"numFiles\")").get(0).trim());
        Assert.assertEquals("true", hiveShell.executeQuery("show tblproperties dealer_hierarchy_delta(\"STATS_GENERATED_VIA_STATS_TASK\")").get(0).trim());
        Assert.assertEquals("153086", hiveShell.executeQuery("show tblproperties dealer_hierarchy_delta(\"totalSize\")").get(0).trim());
    }

    @Test
    public void testSelectFromCtas() {
        List<String> expected = Arrays.asList("GENIUS_SPECTRUM", "KROGERS", "MILESTONE", "SYSCO");
        List<String> actual = hiveShell.executeQuery("select store_name from dealer_hierarchy_delta_prim order by store_name");

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testDataCountFromDeltaTableTextFormat() {
        List<String> expected = Arrays.asList("4");
        List<String> actual = hiveShell.executeQuery("select count(*) from dealer_hierarchy_delta");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testDeltaTableContentByHash() {

        String expectedHash = null;
        String actualHash = null;

        System.out.println("Dealer_Hierarchy : Expected Row :-> " + this.dataFromString.replace('|', '\t'));

        try {
            expectedHash = Sha1Hex.makeSHA1Hash(this.dataFromString.replace('|', '\t'));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        System.out.println("Dealer_Hierarchy : Expected Hash :-> " + expectedHash);

        List<String> actual = hiveShell.executeQuery("select * from dealer_hierarchy_delta where store_name = 'GENIUS_SPECTRUM'");
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
}
