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

package com.cat.ddsw.hivetest.udf;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

@RunWith(StandaloneHiveRunner.class)
public class HiveUDFTest {

//    @HiveSetupScript
//    File setupFile = new File("src/test/resources/common/set_hive_properties.hql");

    @HiveSQL(files = {}, autoStart = true)
    public HiveShell hiveShell;

    @Test
    public void testUDFsCreated() {
        String expected1 = "custom_utc_timestamp";
        String expected2 = "format_mr_timestamp";

        HiveUDFUtil.loadHiveUDFs(hiveShell);

        List<String> actual = hiveShell.executeQuery("show functions");

        System.out.println("UDF : CUSTOM_UTC_TIMESTAMP :-> " + actual.contains(expected1));
        System.out.println("UDF : FORMAT_MR_TIMESTAMP :-> " + actual.contains(expected2));

        Assert.assertTrue(actual.contains(expected1));
        Assert.assertTrue(actual.contains(expected2));
    }
}

