package task;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;

import utils.Utils;

import main.HBShell;

public class Task_put extends TaskBase {
    public Task_put(Map<String, Object> patternMap) {
        super(patternMap);
    }

    @Override
    public void go()
    throws IOException {
        String tableName     = patternMap.get(HBShell.TABLE_NAME).toString();
        String rowKey        = patternMap.get(HBShell.ROW_KEY).toString();
        String familyName    = patternMap.get(HBShell.FAMILY_NAME).toString();
        String qualifierName = patternMap.get(HBShell.QUALIFIER_NAME).toString();
        String value         = patternMap.get(HBShell.VALUE).toString();

        HTable hTable = null;

        try {
            if (!Utils.tableExists(tableName)) {
                throw new NoSuchColumnFamilyException("Table '" + tableName + "' not found");
            }

            hTable = Utils.getTable(tableName);

            if (!Utils.getFamilies(hTable).contains(familyName)) {
                throw new NoSuchColumnFamilyException("Family '" + familyName + "' does not exist in table '" + tableName + "'");
            }

            Utils.put(hTable, rowKey, familyName, qualifierName, value);
        } catch (TableNotFoundException e) {
            log.error(null, e);
        } catch (NoSuchColumnFamilyException e) {
            log.error(null, e);
        } finally {
            if (hTable != null) {
                hTable.close();
            }
        }
    }
}
