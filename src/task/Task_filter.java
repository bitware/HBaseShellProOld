package task;

import java.io.IOException;
import java.util.Map;

public class Task_filter extends TaskBase {
    public Task_filter(Map<String, Object> patternMap) {
        super(patternMap);
    }

    @Override
    public void go()
    throws IOException {
        travelDatabase();
    }
}
