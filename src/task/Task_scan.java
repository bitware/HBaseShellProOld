package task;

import java.io.IOException;
import java.util.Map;

public class Task_scan extends TaskBase {
    public Task_scan(Map<String, Object> patternMap) {
        super(patternMap);
    }

    @Override
    public void go()
    throws IOException {
        travelDatabase();
    }
}
