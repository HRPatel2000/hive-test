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

package com.cat.ddsw.hivetest.ddl_repair;

import com.klarna.hiverunner.HiveShell;

public class DDLRepairUtil {

    public static void repairTables(HiveShell hiveShell, String... tableNames) {

        for(String tableName : tableNames) {

            hiveShell.execute("MSCK REPAIR TABLE " + tableName);
            System.out.println("Hive Partition Repaired for Table : " + tableName);
        }
    }
}

