package task;

import java.io.IOException;
import java.util.Map;
import main.HBShell;

public class Task_describe extends TaskBase {
    public Task_describe(Map<String, Object> patternMap) {
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

        for (String familyName : currentFamilies) {
            if (isMatch(HBShell.FAMILY_NAME, familyName)) {
                super.foundFamilyName(tableName, null, familyName);
            }
        }
    }
}
