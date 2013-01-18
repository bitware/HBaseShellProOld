package task;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import main.HBShell;

import org.apache.hadoop.hbase.client.HTable;

import tnode.TNodeBase;
import tnode.TNodeDatabase;
import tnode.TNodeRow;
import tnode.TNodeTable;
import utils.ResultLog;

public abstract class TaskBase implements Task {
    public enum TaskType {
        CLEAR,
        CONNECT,
        CREATE,
        DELETE,
        DESCRIBE,
        FILTER,
        GET,
        HELP,
        HISTORY,
        LIST,
        PUT,
        QUIT,
        RENAME,
        SCAN,
        VERSION,
    }

    public enum Level {
        TABLE,
        ROW,
        FAMILY,
        QUALIFIER,
        VALUE,
        OTHER,
    }

    private static final String CLASS_NAME_PREFIX = "task.Task_";

    protected static final ResultLog log = ResultLog.getLog();

    public Map<Level, Object> levelParam = new HashMap<Level, Object>();
    public Level              level      = null;

    private boolean notifyEnabled = false;
    private boolean needConfirm   = false;

    private static Map<String, TaskType> aliasMap = null;
    private static boolean               forced   = false;

    private TaskType taskType = null;

    @Override
    public final void printHelp() {
        log.info(getTaskType() + " - " + description());
        log.info("");
        log.info("  usage   : " + usage());
        log.info("  example : " + example());
        log.info("  alias   : " + alias());
        log.info("");
    }

    protected abstract String description();
    protected abstract String usage();

    @Override
    public List< ? > alias() {
        String aliasName = "alias_" + getTaskName();

        try {
            Field field = HBShell.class.getField(aliasName);
            return (List< ? >)field.get(null);
        } catch (Exception e) {     // all exceptions
            log.warn(null, e);
        }

        return null;
    }

    @Override
    public final void doTask(String[] args)
    throws IOException {
        changeLogOnStart();

        parseArgs(args);

        if (!forced && needConfirm) {
            if (!HBShell.confirmFor("Sure to " + getTaskType() + "?")) {
                return;
            }
        }

        execute();
    }

    protected void changeLogOnStart() {
        log.startNew();
    }

    protected final void parseArgs(String[] args)
    throws IOException {
        if (!checkArgNumber(args.length)) {
            throw new IOException("Invalid argument number '" + args.length + "'");
        }

        // levelParam
        assignParam(args);

        // level
        this.level = getLevel();

        // needConfirm
        this.needConfirm = needConfirm();

        // notifyEnabled
        this.notifyEnabled = notifyEnabled();

        // output
        outputParam();
    }

    protected abstract boolean checkArgNumber(int argNumber);

    protected void assignParam(String[] args) {
        try {
            levelParam.put(Level.TABLE,     Pattern.compile(args[0]));
            levelParam.put(Level.ROW,       Pattern.compile(args[1]));
            levelParam.put(Level.FAMILY,    Pattern.compile(args[2]));
            levelParam.put(Level.QUALIFIER, Pattern.compile(args[3]));
            levelParam.put(Level.VALUE,     Pattern.compile(args[4]));
        } catch (ArrayIndexOutOfBoundsException e) {
            // OK
        }
    }

    protected Level getLevel() {
        return null;
    }

    protected boolean needConfirm() {
        return false;
    }

    protected boolean notifyEnabled() {
        return false;
    }

    private void outputParam() {
        log.info("taskType        : " + getTaskType());

        if (level != null) {
            log.info("level           : " + level);
        }

        if (levelParam.get(Level.TABLE) != null) {
            log.info("param-Table     : " + levelParam.get(Level.TABLE));
        }

        if (levelParam.get(Level.ROW) != null) {
            log.info("param-RowKey    : " + levelParam.get(Level.ROW));
        }

        if (levelParam.get(Level.FAMILY) != null) {
            log.info("param-Family    : " + levelParam.get(Level.FAMILY));
        }

        if (levelParam.get(Level.QUALIFIER) != null) {
            log.info("param-Qualifier : " + levelParam.get(Level.QUALIFIER));
        }

        if (levelParam.get(Level.VALUE) != null) {
            log.info("param-Value     : " + levelParam.get(Level.VALUE));
        }

        if (levelParam.get(Level.OTHER) != null) {
            log.info("param-Other     : " + levelParam.get(Level.OTHER));
        }

        log.info("---------------------------------------");
    }

    public void execute()
    throws IOException {
        new TNodeDatabase(this).handle();
    }

    //
    // utils
    //

    private static String getTaskClassName(TaskType taskType)
    throws ClassNotFoundException {
        return CLASS_NAME_PREFIX + taskType.toString().toLowerCase();
    }

    private TaskType getTaskType() {
        if (taskType != null) {
            return taskType;
        }

        this.taskType = getAliasMap().get(getTaskName());
        return taskType;
    }

    private String getTaskName() {
        String className = getClass().getName();
        return className.substring(CLASS_NAME_PREFIX.length());
    }

    public static final TaskType getTaskType(String string)
    throws IOException {
        String   command  = checkIfForced(string);
        TaskType taskType = getAliasMap().get(command);

        if (taskType == null) {
            throw new IOException("Undefined command '" + command.toUpperCase() + "'");
        }

        return taskType;
    }

    private static String checkIfForced(String string) {
        if (string.endsWith("!")) {
            forced = true;
            return string.substring(0, string.length() - 1);
        } else {
            forced = false;
            return string;
        }
    }

    public static final Task getTask(TaskType taskType) {
        try {
            String     taskClassName = getTaskClassName(taskType);
            Class< ? > clazz         = Class.forName(taskClassName);

            Class< ? > [] parameterTypes = new Class[] {};
            Constructor< ? > constructor = clazz.getConstructor(parameterTypes);

            return (Task) constructor.newInstance(new Object[] {});
        } catch (Exception e) {         // all exceptions
            // a lot of exceptions, but there should be no errors if all taskType implemented
            log.error(null, e);
            return null;
        }
    }

    public static Map<String, TaskType> getAliasMap() {
        if (aliasMap != null) {
            return aliasMap;
        }

        aliasMap = new HashMap<String, TaskType>();

        for (TaskType taskType : TaskType.values()) {
            Task      task    = TaskBase.getTask(taskType);
            List< ? > aliases = task.alias();

            for (Object alias : aliases) {
                aliasMap.put((String) alias, taskType);
            }

            aliasMap.put(taskType.toString().toLowerCase(), taskType);
        }

        return aliasMap;
    }

    //
    // notify
    //

    public void notifyFound(TNodeBase node)
    throws IOException {
        if (!notifyEnabled) {
            return;
        }

        HTable table = null;

        switch (node.level) {
        case TABLE :
            table = ((TNodeTable)node).getTable();

            try {
                foundTable(table);
            } finally {
                ((TNodeTable)node).closeTable(table);
            }

            break;

        case ROW :
            table = ((TNodeRow)node).table;
            foundRow(table, node.name);
            break;

        case FAMILY :
            table = ((TNodeRow)node.parent).table;
            foundFamily(table, node.parent.name, node.name);
            break;

        case QUALIFIER :
            table = ((TNodeRow)node.parent.parent).table;
            foundQualifier(table, node.parent.parent.name, node.parent.name, node.name);
            break;

        case VALUE:
            table = ((TNodeRow)node.parent.parent.parent).table;
            foundValue(table, node.parent.parent.parent.name, node.parent.parent.name, node.parent.name, node.name);
            break;

        default:
            break;
        }
    }

    protected void foundTable(HTable table)
    throws IOException {
        // Do nothing
    }

    protected void foundRow(HTable table, String row)
    throws IOException {
        // Do nothing
    }

    protected void foundFamily(HTable table, String row, String family)
    throws IOException {
        // Do nothing
    }

    protected void foundQualifier(HTable table, String row, String family, String qualifier)
    throws IOException {
        // Do nothing
    }

    protected void foundValue(HTable table, String row, String family, String qualifier, String value)
    throws IOException {
        // Do nothing
    }

    public boolean isMatch(Level level, String target) {
        Pattern pattern = (Pattern)levelParam.get(level);

        if (pattern == null) {
            return true;
        }

        Matcher matcher = pattern.matcher(target);
        return matcher.find();
    }

    public boolean isGet() {
        return getTaskType() == TaskType.GET;
    }

    public boolean isFilter() {
        return getTaskType() == TaskType.FILTER;
    }
}
