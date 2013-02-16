package task;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableExistsException;

import utils.Utils;

import main.HBShell;

public class Task_create extends TaskBase {
    public Task_create(Map<String, Object> patternMap) {
        super(patternMap);
    }

    @Override
    public void go()
    throws MasterNotRunningException, IOException {
        String    tableName = patternMap.get(HBShell.TABLE_NAME).toString();
        List< ? > families  = (List< ? >)patternMap.get(HBShell.FAMILY_NAME);

        try {
            if (Utils.tableExists(tableName)) {
                throw new TableExistsException("Table '" + tableName + "' already exists");
            }

            Utils.createTable(tableName, families);
        } catch (TableExistsException e) {
            log.error(null, e);
        }
    }
}
