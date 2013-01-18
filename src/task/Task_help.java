package task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Task_help extends TaskBase {
    @Override
    protected String description() {
        return "show help message";
    }

    @Override
    protected String usage() {
        return "help [topic1 [topic2 ...]]";
    }

    @Override
    public String example() {
        return "help filter get";
    }

    @Override
    protected boolean checkArgNumber(int argNumber) {
        return argNumber >= 0;
    }

    @Override
    protected void assignParam(String[] args) {
        List<String> topics = new ArrayList<String>();

        for (int i = 0; i < args.length; i++) {
            topics.add(args[i]);
        }

        levelParam.put(Level.OTHER, topics);
    }

    @Override
    public void execute()
    throws IOException {
        List< ? > topics = (List< ? >)levelParam.get(Level.OTHER);

        if (topics.isEmpty()) {
            printAllHelp();
        } else {
            printHelpOn(topics);
        }
    }

    private void printAllHelp() {
        for (TaskType taskType : TaskType.values()) {
            printHelpOn(taskType);
        }

        printSpecialNote();
    }

    private void printSpecialNote() {
        log.info("** NOTE" + " - " + "Keyboard in linux");
        log.info("");
        log.info(" - all control keys are not usable before jline added");
        log.info(" - thanks to jline, arrow left/right/up/down are usable, but");
        log.info(" - backspace and delete are switched (resolved by MyUnixTerminal)");
        log.info(" - home/end, page up/down are not usable (resolved by MyConsoleReader partially)");
        log.info("  - the following text in pasting text will act as control keys");
        log.info("   - '1~' -> home, go to begin of line");
        log.info("   - '2~' -> insert, do nothing");
        log.info("   - '4~' -> end, go to end of line");
        log.info("   - '5~' -> page up, move to first history entry");
        log.info("   - '6~' -> page down, move to last history entry");
        log.info("");
    }

    private void printHelpOn(List< ? > topics)
    throws IOException {
        for (Object topic : topics) {
            TaskType taskType = getTaskType(topic.toString());
            printHelpOn(taskType);
        }
    }

    private void printHelpOn(TaskType taskType) {
        Task task = TaskBase.getTask(taskType);
        task.printHelp();
    }
}
