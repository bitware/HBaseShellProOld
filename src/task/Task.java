package task;

import java.io.IOException;
import java.util.List;

public interface Task {
    void doTask(String[] args)
    throws IOException;

    void printHelp();

    List< ? > alias();

    String example();
}
