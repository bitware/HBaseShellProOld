package task;

import java.io.IOException;
import java.util.Map;

public class Task_list extends TaskBase {
    public Task_list(Map<String, Object> patternMap) {
        super(patternMap);
    }

    @Override
    public void go()
    throws IOException {
        travelDatabase();
    }
}
