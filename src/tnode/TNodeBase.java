package tnode;

import java.io.IOException;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import task.TaskBase;
import task.TaskBase.Level;
import utils.ResultLog;

public abstract class TNodeBase implements TNode {
    protected static final String FILE_DATA_QUALIFIER_PATTERN = "^f\\d+$";

    protected final TaskBase task;

    public final TNodeBase parent;
    public final String    name;
    public final Level     level;

    protected static final ResultLog log = ResultLog.getLog();

    protected boolean outputted         = false;
    private Boolean   otherFilterPassed = null;

    protected TNodeBase(TaskBase task, TNodeBase parent, String name, Level level) {
        this.task   = task;
        this.parent = parent;
        this.name   = name;
        this.level  = level;
    }

    @Override
    public void handle()
    throws IOException {
        if (level == task.level) {
            output();
        } else {
            travelChildren();
        }
    }

    public void output()
    throws IOException {
        if (outputted) {
            return;
        }

        this.outputted = true;

        if (parent != null) {
            parent.output();
        }

        if (name != null) {
            log.info(String.format(formatString(), name));

            if (task != null) {
                task.notifyFound(this);
            }
        }
    }

    protected boolean otherFilterPassed() {
        if (otherFilterPassed != null) {
            return otherFilterPassed;
        }

        if (!task.isFilter()) {
            this.otherFilterPassed = true;
            return true;
        }

        if ((parent != null) && (parent.otherFilterPassed())) {
            this.otherFilterPassed = true;
            return true;
        }

        if ((name != null) && (task.isMatch(Level.OTHER, name))) {
            this.otherFilterPassed = true;
            return true;
        }

        this.otherFilterPassed = false;
        return false;
    }

    //
    // abstract methods
    //

    protected abstract String formatString();

    protected abstract void travelChildren()
    throws IOException;

    //
    // filter
    //

    // file data qualifier filter
    public static CompareFilter getFileDataQualifierFilter(boolean equal) {
        RegexStringComparator comparator = new RegexStringComparator(FILE_DATA_QUALIFIER_PATTERN);
        CompareOp             compareOp  = equal ? CompareOp.EQUAL : CompareOp.NOT_EQUAL;
        return new QualifierFilter(compareOp, comparator);
    }

    // qualifier pattern filter
    protected CompareFilter getQualifierPatternFilter() {
        Object pattern = task.levelParam.get(Level.QUALIFIER);

        if (pattern != null) {
            RegexStringComparator comparator = new RegexStringComparator(pattern.toString());
            return new QualifierFilter(CompareOp.EQUAL, comparator);
        }

        return null;
    }

    // value pattern filter
    protected CompareFilter getValuePatternFilter() {
        Object pattern = task.levelParam.get(Level.VALUE);

        if (pattern != null) {
            RegexStringComparator comparator = new RegexStringComparator(pattern.toString());
            return new ValueFilter(CompareOp.EQUAL, comparator);
        }

        return null;
    }
}
