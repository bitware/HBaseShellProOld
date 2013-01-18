package task;

public class Task_quit extends TaskBase {
    @Override
    protected String description() {
        return "quit this shell";
    }

    @Override
    protected String usage() {
        return "quit";
    }

    @Override
    public String example() {
        return "quit";
    }

    @Override
    protected void changeLogOnStart() {
        log.enableInfo(false);
    }

    @Override
    protected boolean checkArgNumber(int argNumber) {
        return argNumber == 0;
    }

    @Override
    public void execute() {
        System.exit(0);
    }
}
