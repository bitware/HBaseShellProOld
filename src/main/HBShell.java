package main;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Scanner;
import java.util.StringTokenizer;

import jline.ConsoleReader;

import org.apache.commons.io.FileUtils;

import task.Task;
import task.TaskBase;
import task.Task_history;
import task.TaskBase.TaskType;
import utils.MyConsoleReader;
import utils.PropertiesHelper;
import utils.ResultLog;
import utils.RootLog;
import utils.Utils;

public class HBShell {
    public static final String HISTORY_FILE = Utils.makePath(RootLog.logDir, "history.txt");

    private static final String[] DEFAULT_CMD_ARGS_FOR_SINGLE_SESSION = new String[] {"version"};

    private static final String CONFIRM_YES    = "yes";
    private static final String ENCODING       = "UTF-8";
    private static final String LINE_SEPARATOR = System.getProperty("line.separator", "\n");

    public enum SessionMode {
        auto,
        single,
        multi,
    }

    private static final String MAIN_CONF_FILE = "./conf/config.ini";

    private static final ResultLog log         = ResultLog.getLog();
    private static final File      historyFile = new File(HISTORY_FILE);

    // config default values
    public static Long        maxPrintableDetectCnt   = 1000L;
    public static Long        maxHexStringLength      = 8L;
    public static Boolean     travelRowFBlockFamilies = true;
    public static SessionMode sessionMode             = SessionMode.auto;
    public static Long        maxResultLogFileCount   = 10L;
    public static Long        defaultHistoryCount     = 30L;

    public static List<String> alias_clear    = Arrays.asList("c", "cle", "clr");
    public static List<String> alias_connect  = Arrays.asList("con");
    public static List<String> alias_create   = Arrays.asList("cre");
    public static List<String> alias_delete   = Arrays.asList("del");
    public static List<String> alias_describe = Arrays.asList("d", "des");
    public static List<String> alias_filter   = Arrays.asList("f");
    public static List<String> alias_get      = Arrays.asList("g");
    public static List<String> alias_help     = Arrays.asList("h");
    public static List<String> alias_history  = Arrays.asList("his");
    public static List<String> alias_list     = Arrays.asList("l", "ls");
    public static List<String> alias_put      = Arrays.asList("p");
    public static List<String> alias_quit     = Arrays.asList("e", "q", "exit");
    public static List<String> alias_rename   = Arrays.asList("r", "ren");
    public static List<String> alias_scan     = Arrays.asList("s");
    public static List<String> alias_version  = Arrays.asList("v", "ver");

    public static String format_table         = "T: %s";
    public static String format_row           = " R: %s";
    public static String format_family        = "  F: %s";
    public static String format_qualifier     = "   Q: %14s";
    public static String format_qualifierOmit = "   %s";
    public static String format_value         = " = %s";

    private static Scanner       inputScanner  = null; // for windows
    private static ConsoleReader consoleReader = null; // for linux
    private static String        lastCmd       = Task_history.getLastCmd();

    private static void init() {
        // read main configure file
        Properties properties = PropertiesHelper.getProperties(MAIN_CONF_FILE);

        maxPrintableDetectCnt   = PropertiesHelper.getProperty(properties, "maxPrintableDetectCnt",     maxPrintableDetectCnt);
        maxHexStringLength      = PropertiesHelper.getProperty(properties, "maxHexStringLength",        maxHexStringLength);
        travelRowFBlockFamilies = PropertiesHelper.getProperty(properties, "travelRowFBlockFamilies",   travelRowFBlockFamilies);
        sessionMode             = PropertiesHelper.getProperty(properties, "sessionMode",               sessionMode);
        maxResultLogFileCount   = PropertiesHelper.getProperty(properties, "maxResultLogFileCount",     maxResultLogFileCount);
        defaultHistoryCount     = PropertiesHelper.getProperty(properties, "defaultHistoryCount",       defaultHistoryCount);

        alias_clear    = PropertiesHelper.getProperty(properties, "alias_clear",    alias_clear);
        alias_connect  = PropertiesHelper.getProperty(properties, "alias_connect",  alias_connect);
        alias_create   = PropertiesHelper.getProperty(properties, "alias_create",   alias_create);
        alias_delete   = PropertiesHelper.getProperty(properties, "alias_delete",   alias_delete);
        alias_describe = PropertiesHelper.getProperty(properties, "alias_describe", alias_describe);
        alias_filter   = PropertiesHelper.getProperty(properties, "alias_filter",   alias_filter);
        alias_get      = PropertiesHelper.getProperty(properties, "alias_get",      alias_get);
        alias_help     = PropertiesHelper.getProperty(properties, "alias_help",     alias_help);
        alias_history  = PropertiesHelper.getProperty(properties, "alias_history",  alias_history);
        alias_list     = PropertiesHelper.getProperty(properties, "alias_list",     alias_list);
        alias_put      = PropertiesHelper.getProperty(properties, "alias_put",      alias_put);
        alias_quit     = PropertiesHelper.getProperty(properties, "alias_quit",     alias_quit);
        alias_rename   = PropertiesHelper.getProperty(properties, "alias_rename",   alias_rename);
        alias_scan     = PropertiesHelper.getProperty(properties, "alias_scan",     alias_scan);
        alias_version  = PropertiesHelper.getProperty(properties, "alias_version",  alias_version);

        format_table         = removeQuotes(PropertiesHelper.getProperty(properties, "format_table",         format_table));
        format_row           = removeQuotes(PropertiesHelper.getProperty(properties, "format_row",           format_row));
        format_family        = removeQuotes(PropertiesHelper.getProperty(properties, "format_family",        format_family));
        format_qualifier     = removeQuotes(PropertiesHelper.getProperty(properties, "format_qualifier",     format_qualifier));
        format_qualifierOmit = removeQuotes(PropertiesHelper.getProperty(properties, "format_qualifierOmit", format_qualifierOmit));
        format_value         = removeQuotes(PropertiesHelper.getProperty(properties, "format_value",         format_value));

        if (sessionMode == SessionMode.auto) {
            sessionMode = Utils.isLinux() ? SessionMode.single : SessionMode.multi;
        }
    }

    private static String removeQuotes(String string) {
        return string.substring(1, string.length() - 1);
    }

    private static void exit() {
        closeInputScanner();
    }

    private static void doTask(String[] cmdArgs)
    throws IOException {
        TaskType taskType = TaskBase.getTaskType(cmdArgs[0]);
        Task     task     = TaskBase.getTask(taskType);

        String[] args = new String[cmdArgs.length - 1];
        System.arraycopy(cmdArgs, 1, args, 0, cmdArgs.length - 1);        // remove first arg(task type)

        task.doTask(args);
    }

    public static void main(String[] args)
    throws IOException {
        init();

        do {
            String[] cmdArgs = (sessionMode == SessionMode.single) ? args : getCmdArgs();

            if (cmdArgs.length == 0) {
                if (sessionMode == SessionMode.single) {
                    cmdArgs = DEFAULT_CMD_ARGS_FOR_SINGLE_SESSION;
                } else {
                    continue;
                }
            }

            Date start = new Date();

            try {
                doTask(cmdArgs);
            } catch (Exception e) {         // all exceptions
                log.error(null, e);

                if (sessionMode == SessionMode.single) {
                    break;
                } else {
                    continue;
                }
            }

            Date stop = new Date();

            double timeUsed = (stop.getTime() - start.getTime()) / 1000.0;

            log.info("---------------------------------------");
            log.stopLogToFile();
            log.info("時間　　    ：" + timeUsed + " [sec]");
            log.info("");

            historyAdd(cmdArgs);
        } while (sessionMode == SessionMode.multi);

        exit();
    }

    private static void historyAdd(String[] cmdArgs)
    throws IOException {
        String cmd = Utils.join(cmdArgs, " ");

        if (!cmd.equals(lastCmd)) {
            lastCmd = cmd;
            FileUtils.writeStringToFile(historyFile, cmd + LINE_SEPARATOR, ENCODING, true);
        }
    }

    public static boolean confirmFor(String message)
    throws IOException {
        String userInput = getUserInput(message + " \t" + CONFIRM_YES + "/[no] : ");
        return userInput.equals(CONFIRM_YES);
    }

    private static String[] getCmdArgs()
    throws IOException {
        return getTokens(getUserInput("> "));
    }

    private static String getUserInput(String prompt)
    throws IOException {
        String line = null;

        if (Utils.isLinux()) {
            line = getConsoleReader().readLine(prompt);
        } else {
            System.out.print(prompt);

            try {
                line = getInputScanner().nextLine();
            } catch (NoSuchElementException e) {
                // user may press ^C (first input)
                // the following lines may not be run
                log.warn(null, e);
                System.exit(-1);
            }
        }

        return line;
    }

    private static Scanner getInputScanner() {
        if (inputScanner != null) {
            return inputScanner;
        }

        inputScanner = new Scanner(System.in);
        return inputScanner;
    }

    private static ConsoleReader getConsoleReader()
    throws IOException {
        if (consoleReader != null) {
            return consoleReader;
        }

        consoleReader = new MyConsoleReader();
        consoleReader.setBellEnabled(false);    // bell does not work correctly, so disable it

        return consoleReader;
    }

    private static void closeInputScanner() {
        if (inputScanner != null) {
            inputScanner.close();
            inputScanner = null;
        }
    }

    private static String[] getTokens(String input) {
        StringTokenizer st     = new StringTokenizer(input);
        List<String>    tokens = new ArrayList<String>();

        while (st.hasMoreTokens()) {
            tokens.add(st.nextToken());
        }

        return tokens.toArray(new String[tokens.size()]);
    }
}
