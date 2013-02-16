package main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import task.Task;
import task.TaskBase.TravelLevel;
import task.TaskBase.TaskType;
import utils.ResultLog;
import utils.Utils;

public class HBShell {
    public enum SessionMode {
        auto,
        single,
        multi,
    }

    private static final String MAIN_CONF_FILE = "./conf/config.ini";
    private static final ResultLog log         = ResultLog.getLog();

    public static final String TABLE_NAME     = "TableName";
    public static final String ROW_KEY        = "RowKey";
    public static final String FAMILY_NAME    = "FamilyName";
    public static final String QUALIFIER_NAME = "QualifierName";
    public static final String VALUE          = "Value";
    public static final String COMMON         = "Common";

    public static Long        maxPrintableDetectCnt   = 1000L;
    public static Long        maxHexStringLength      = 8L;
    public static Boolean     travelRowFBlockFamilies = true;
    public static SessionMode sessionMode             = SessionMode.auto;
    public static Long        MaxResultLogFileCount   = 10L;

    private static Scanner inputScanner = null;

    private static void init()
    throws FileNotFoundException, IOException {
        // read main configure file
        Properties properties = new Properties();
        properties.load(new FileInputStream(MAIN_CONF_FILE));

        maxPrintableDetectCnt   = Long.valueOf(properties.getProperty("maxPrintableDetectCnt", maxPrintableDetectCnt.toString()));
        maxHexStringLength      = Long.valueOf(properties.getProperty("maxHexStringLength", maxHexStringLength.toString()));
        travelRowFBlockFamilies = Boolean.valueOf(properties.getProperty("travelRowFBlockFamilies", travelRowFBlockFamilies.toString()));
        sessionMode             = SessionMode.valueOf(properties.getProperty("sessionMode", sessionMode.toString()));
        MaxResultLogFileCount   = Long.valueOf(properties.getProperty("MaxResultLogFileCount", MaxResultLogFileCount.toString()));

        if (sessionMode == SessionMode.auto) {
            sessionMode = Utils.isLinux() ? SessionMode.single : SessionMode.multi;
        }

        if (sessionMode == SessionMode.multi) {
            inputScanner = new Scanner(System.in);
        }
    }

    private static void exit() {
        if (inputScanner != null) {
            inputScanner.close();
        }
    }

    public static TaskType    taskType    = null;
    public static TravelLevel travelLevel = null;

    private static Map<String, Object> patternMap = null;
    private static String              helpTopic  = null;

    private static void initCmd() {
        taskType    = null;
        travelLevel = null;
        patternMap  = new HashMap<String, Object>();
        helpTopic   = null;
    }

    private static Map<String, String[]> helpMap = null;

    private static void printHelp()
    throws IOException {
        if (helpMap == null) {
            helpMap = new HashMap<String, String[]>();

            helpMap.put("list",
                        new String[] {
                            "list database data at a specified level",
                            "list [table_name_pattern [row_key_pattern [family_name_pattern [qualifier_name_pattern [value_pattern]]]]]"
                        });
            helpMap.put("scan",
                        new String[] {
                            "scan database data with given filter",
                            "scan [table_name_pattern [row_key_pattern [family_name_pattern [qualifier_name_pattern [value_pattern]]]]]"
                        });
            helpMap.put("filter",
                        new String[] {
                            "filter database data with given filter, common_pattern(OR) will be used if some pattern does not exist",
                            "filter [table_name_pattern [row_key_pattern [family_name_pattern [qualifier_name_pattern [value_pattern]]]]] common_pattern"
                        });
            helpMap.put("put",
                        new String[] {
                            "put a cell 'value' at specified table/row/family:qualifier",
                            "put table_name row_key family_name qualifier_name value"
                        });
            helpMap.put("delete",
                        new String[] {
                            "delete data in database with given filter",
                            "delete [table_name_pattern [row_key_pattern [family_name_pattern [qualifier_name_pattern [value_pattern]]]]]",
                        });
            helpMap.put("create",
                        new String[] {
                            "create table",
                            "create table_name family_name1 [family_name2 ...]"
                        });
            helpMap.put("clear",
                        new String[] {
                            "clear table",
                            "clear [table_name_pattern]"
                        });
            helpMap.put("describe",
                        new String[] {
                            "describe the named table",
                            "describe [table_name_pattern [family_name_pattern]]"
                        });
            helpMap.put("exit",
                        new String[] {
                            "exit the shell",
                            "exit"
                        });
            helpMap.put("quit",
                        new String[] {
                            "exit the shell",
                            "exit"
                        });
            helpMap.put("help",
                        new String[] {
                            "show help message",
                            "help [helpTopic]"
                        });
        }

        if (helpTopic == null) {
            for (String key : helpMap.keySet()) {
                printHelpOn(key);
            }
        } else if (helpMap.get(helpTopic) == null) {
            log.warn("No help on '" + helpTopic + "' found!");
        } else {
            printHelpOn(helpTopic);
        }
    }

    private static void printHelpOn(String helpTopic)
    throws IOException {
        String[] body = helpMap.get(helpTopic);

        String info  = body[0];
        String usage = body[1];

        log.info(helpTopic + " - " + info);
        log.info(usage);
        log.info("");
    }

    private static boolean parseArgs(String[] args)
    throws IOException {
        log.startNew();

        if (args.length == 0) {
            return false;
        }

        // task type
        try {
            taskType = TaskType.valueOf(args[0].toUpperCase());
        } catch (IllegalArgumentException e) {
            log.error(null, e);
            return false;
        }

        // argument number

        if (taskType == TaskType.PUT && args.length != 6 ||
            taskType == TaskType.CREATE && args.length < 3 ||
            taskType == TaskType.FILTER && args.length < 2)
        {
            try {
                throw new Exception("invalid argument number '" + args.length + "'");
            } catch (Exception e) {
                log.error(null, e);
                return false;
            }
        }

        // special task: help
        if (taskType == TaskType.HELP) {
            if (args.length == 2) {
                helpTopic = args[1];
            }

            return true;
        }

        // special task: exit / quit
        if (taskType == TaskType.EXIT || taskType == TaskType.QUIT) {
            return true;
        }

        // travelLevel
        if (taskType == TaskType.LIST || taskType == TaskType.DELETE) {
            travelLevel = TravelLevel.TABLE;

            if (args.length >= 2) {
                travelLevel = TravelLevel.values()[args.length - 2];
            }
        }

        if (taskType == TaskType.DESCRIBE || taskType == TaskType.CLEAR) {
            travelLevel = TravelLevel.TABLE;
        }

        if (taskType == TaskType.SCAN || taskType == TaskType.FILTER) {
            travelLevel = TravelLevel.VALUE;
        }

        // table_name row_key family_name qualifier_name value pattern
        try {
            if (taskType == TaskType.CREATE) {
                List<String> families = new ArrayList<String>();

                for (int i = 2; i < args.length; i++) {
                    families.add(args[i]);
                }

                patternMap.put(TABLE_NAME,        args[1]);
                patternMap.put(FAMILY_NAME,       families);
            } else if (taskType == TaskType.DESCRIBE) {
                patternMap.put(TABLE_NAME,        Pattern.compile(args[1]));
                patternMap.put(FAMILY_NAME,       Pattern.compile(args[2]));
            } else if (taskType == TaskType.PUT) {
                patternMap.put(TABLE_NAME,        args[1]);
                patternMap.put(ROW_KEY,           args[2]);
                patternMap.put(FAMILY_NAME,       args[3]);
                patternMap.put(QUALIFIER_NAME,    args[4]);
                patternMap.put(VALUE,             args[5]);
            } else if (taskType == TaskType.FILTER) {
                patternMap.put(COMMON,            Pattern.compile(args[args.length - 1]));

                String[] args2 = new String[args.length - 1];
                System.arraycopy(args, 0, args2, 0, args.length - 1);    // remove last arg

                patternMap.put(TABLE_NAME,        Pattern.compile(args2[1]));
                patternMap.put(ROW_KEY,           Pattern.compile(args2[2]));
                patternMap.put(FAMILY_NAME,       Pattern.compile(args2[3]));
                patternMap.put(QUALIFIER_NAME,    Pattern.compile(args2[4]));
                patternMap.put(VALUE,             Pattern.compile(args2[5]));
            } else {
                patternMap.put(TABLE_NAME,        Pattern.compile(args[1]));
                patternMap.put(ROW_KEY,           Pattern.compile(args[2]));
                patternMap.put(FAMILY_NAME,       Pattern.compile(args[3]));
                patternMap.put(QUALIFIER_NAME,    Pattern.compile(args[4]));
                patternMap.put(VALUE,             Pattern.compile(args[5]));
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            // OK
        }

        // output parameter info
        log.info("taskType              : " + taskType);

        if (taskType == TaskType.LIST) {
            log.info("travelLevel           : " + travelLevel);
        }

        if (patternMap.get(TABLE_NAME) != null) {
            log.info("pattern-TableName     : " + patternMap.get(TABLE_NAME));
        }

        if (patternMap.get(ROW_KEY) != null) {
            log.info("pattern-RowKey        : " + patternMap.get(ROW_KEY));
        }

        if (patternMap.get(FAMILY_NAME) != null) {
            log.info("pattern-FamilyName    : " + patternMap.get(FAMILY_NAME));
        }

        if (patternMap.get(QUALIFIER_NAME) != null) {
            log.info("pattern-QualifierName : " + patternMap.get(QUALIFIER_NAME));
        }

        if (patternMap.get(VALUE) != null) {
            log.info("pattern-Value         : " + patternMap.get(VALUE));
        }

        if (patternMap.get(COMMON) != null) {
            log.info("pattern-Common        : " + patternMap.get(COMMON));
        }

        log.info("---------------------------------------");

        return true;
    }

    private static void doTask()
    throws Exception {
        if (taskType == TaskType.EXIT || taskType == TaskType.QUIT) {
            System.exit(0);
        }

        if (taskType == TaskType.HELP) {
            printHelp();
            return;
        }

        String     taskClassName = "task.Task_" + taskType.toString().toLowerCase();
        Class< ? > clazz         = null;

        try {
            clazz = Class.forName(taskClassName);
        } catch (ClassNotFoundException e) {
            log.error(null, e);
            return;
        }

        Class< ? > [] parameterTypes = new Class[] { Map.class };
        Constructor< ? > constructor = clazz.getConstructor(parameterTypes);

        Task task = (Task) constructor.newInstance(new Object[] { patternMap});
        task.go();
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args)
    throws Exception {
        init();

        String[] cmdArgs = null;

        do {
            initCmd();

            if (sessionMode == SessionMode.single) {
                cmdArgs = args;

                if (!parseArgs(cmdArgs)) {
                    printHelp();
                    break;
                }
            } else {
                System.out.print("> ");
                cmdArgs = getCmdArgs();

                if (!parseArgs(cmdArgs)) {
                    continue;
                }
            }

            Date start = new Date();
            doTask();
            Date stop = new Date();

            double timeUsed = (stop.getTime() - start.getTime()) / 1000.0;

            log.info("---------------------------------------");
            log.stopLogToFile();
            log.info("時間　　    ：" + timeUsed + " [sec]");
            log.info("");
        } while (sessionMode == SessionMode.multi);

        exit();
    }

    private static String[] getCmdArgs()
    throws IOException {
        String line = null;

        try {
            line = inputScanner.nextLine();
        } catch (NoSuchElementException e) {
            // user may press ^C (first input)
            // the following lines may not be run
            log.warn(null, e);
            System.exit(-1);
        }

        String[]  cmdArgs = getTokens(line);
        return cmdArgs;
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
