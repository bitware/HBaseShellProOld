package task;

import java.io.IOException;
import java.util.Map;
import main.HBShell;

import utils.Utils;

public class Task_delete extends TaskBase {
    public Task_delete(Map<String, Object> patternMap) {
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

        if (HBShell.travelLevel == TravelLevel.TABLE) {
            Utils.deleteTable(tableName);
        }
    }

    @Override
    protected void foundRowKey(String tableName, String rowKey)
    throws IOException {
        super.foundRowKey(tableName, rowKey);

        if (HBShell.travelLevel == TravelLevel.ROW) {
            Utils.deleteRow(currentTable, rowKey);
        }
    }

    @Override
    protected void foundFamilyName(String tableName, String rowKey, String familyName)
    throws IOException {
        super.foundFamilyName(tableName, rowKey, familyName);

        if (HBShell.travelLevel == TravelLevel.FAMILY) {
            Utils.deleteFamily(currentTable, rowKey, familyName);
        }
    }

    @Override
    protected void foundQualifierName(String tableName, String rowKey, String familyName, String qualifierName)
    throws IOException {
        super.foundQualifierName(tableName, rowKey, familyName, qualifierName);

        if (HBShell.travelLevel == TravelLevel.QUALIFIER) {
            Utils.deleteQualifier(currentTable, rowKey, familyName, qualifierName);
        }
    }

    @Override
    protected void foundValue(String tableName, String rowKey, String familyName, String qualifierName, String value)
    throws IOException {
        super.foundValue(tableName, rowKey, familyName, qualifierName, value);

        if (HBShell.travelLevel == TravelLevel.VALUE) {
            Utils.deleteQualifier(currentTable, rowKey, familyName, qualifierName);
        }
    }
}
