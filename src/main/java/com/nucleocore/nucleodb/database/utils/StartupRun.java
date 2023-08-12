package com.nucleocore.nucleodb.database.utils;

import com.nucleocore.nucleodb.database.tables.DataTable;

public interface StartupRun {
    void run(DataTable table);
}
