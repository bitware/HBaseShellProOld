package task;

import main.Version;

public class Task_version extends TaskBase {
    private static final int VERSION_MAJOR = 0;
    private static final int VERSION_MINOR = 2;

    @Override
    protected String description() {
        return "show version message";
    }

    @Override
    protected String usage() {
        return "version";
    }

    @Override
    public String example() {
        return "version";
    }

    @Override
    protected boolean checkArgNumber(int argNumber) {
        return argNumber == 0;
    }

    @Override
    public void execute() {
        printVersion();
    }

    private void printVersion() {
        log.info("HBase Shell");
        log.info(" - Simple but powerful replacement for ./hbase shell");
        log.info(" - Designed especially for KeepData database");
        log.info(" - Enter 'help<RETURN>' for list of supported commands");
        log.info("");
        log.info(String.format(" Version  : %d.%d.%s", VERSION_MAJOR, VERSION_MINOR, Version.REVISION));
        log.info(String.format(" Built on : %s", Version.BUILD_TIME));
    }
}
