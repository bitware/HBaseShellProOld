package task;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;

import utils.Utils;

public class Task_clear extends TaskBase {
    public Task_clear(Map<String, Object> patternMap) {
        super(patternMap);
    }

    @Override
    public void go()
    throws IOException {
        travelDatabase();
    }

    @Override
    protected void foundTable(String tableName)
    throws IOException {
        super.foundTable(tableName);

        HTable           hTable     = Utils.getTable(tableName);
        HTableDescriptor descriptor = null;

        try {
            descriptor = hTable.getTableDescriptor();
        } finally {
            hTable.close();
        }

        Utils.deleteTable(tableName);
        Utils.createTable(descriptor);
    }
}
