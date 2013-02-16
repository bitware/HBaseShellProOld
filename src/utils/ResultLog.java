package utils;

import java.io.File;
import java.io.IOException;

import main.HBShell;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ResultLog {
    private static final String    NAME           = "result";
    private static final String    ENCODING       = "UTF-8";
    private static final String    LINE_SEPARATOR = System.getProperty("line.separator", "\n");
    private static final ResultLog LOG            = new ResultLog();

    private File    file          = null;
    private boolean startNew      = false;  // log to a new file
    private boolean stopLogToFile = false;  // logger.result defined in LOG4J_CONF_FILE will go on

    private ResultLog() {
        // call RootLog methods to make sure
        // RootLog configured before all logs output
        RootLog.getLog().trace(RootLog.logDir);
    }

    public static ResultLog getLog() {
        return LOG;
    }

    public void startNew() {
        this.startNew = true;
    }

    public void stopLogToFile() {
        this.stopLogToFile = true;
    }

    public void info(String string)
    throws IOException {
        logger_result().info(string);
        appendToLogFile(string);
    }

    public void warn(String string)
    throws IOException {
        logger_result().warn(string);
        appendToLogFile(string);
    }

    public void warn(String string, Throwable e)
    throws IOException {
        RootLog.getLog().warn(string, e);
        logError(string, e);
    }

    public void error(String string)
    throws IOException {
        logger_result().error(string);
        appendToLogFile(string);
    }

    public void error(String string, Throwable e)
    throws IOException {
        RootLog.getLog().error(string, e);
        logError(string, e);
    }

    // logger.result defined in LOG4J_CONF_FILE
    private static Log logger_result() {
        return LogFactory.getLog(NAME);
    }

    private void logError(String string, Throwable e)
    throws IOException {
        String errorMessage = e.getMessage();

        if (string != null) {
            errorMessage += "[" + string + "]";
        }

        errorMessage += LINE_SEPARATOR;

        logger_result().error(errorMessage);
        appendToLogFile(errorMessage);
    }

    private void appendToLogFile(String string)
    throws IOException {
        if (file == null || startNew) {
            this.file          = new File(newLogFilePath());
            this.startNew      = false;
            this.stopLogToFile = false;
        }

        if (stopLogToFile) {
            return;
        }

        FileUtils.writeStringToFile(file, string + LINE_SEPARATOR, ENCODING, true);
    }

    private static String newLogFilePath() {
        String lastLogFilePath = getLogFilePath(HBShell.MaxResultLogFileCount - 1);

        if (Utils.fileExists(lastLogFilePath)) {
            Utils.deleteFile(getLogFilePath(0));

            for (int i = 1; i < HBShell.MaxResultLogFileCount; i++) {
                Utils.renameFile(getLogFilePath(i), getLogFilePath(i - 1));
            }

            return lastLogFilePath;
        }

        for (long i = HBShell.MaxResultLogFileCount - 2; i >= 0; i--) {
            if (Utils.fileExists(getLogFilePath(i))) {
                return getLogFilePath(i + 1);
            }
        }

        return getLogFilePath(0);
    }

    private static String getLogFilePath(long index) {
        String fileName = NAME + "_" + index + ".hbs";
        return Utils.makePath(RootLog.logDir, fileName);
    }
}
