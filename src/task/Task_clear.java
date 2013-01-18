package task;

import java.io.IOException;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;

import utils.Utils;

public class Task_clear extends TaskBase {
    @Override
    protected String description() {
        return "clear table contents\n" +
               "\n" +
               "** WARNING : 'clear'(and its alias) will clear contents of all tables in database\n" +
               "** NOTE    : use 'clear! ...' to force clear";
    }

    @Override
    protected String usage() {
        return "clear [table_pattern]";
    }

    @Override
    public String example() {
        return "clear ^test_table";
    }

    @Override
    protected boolean checkArgNumber(int argNumber) {
        return argNumber == 0 || argNumber == 1;
    }

    @Override
    protected Level getLevel() {
        return Level.TABLE;
    }

    @Override
    protected boolean needConfirm() {
        return true;
    }

    @Override
    protected boolean notifyEnabled() {
        return true;
    }

    @Override
    protected void foundTable(HTable table)
    throws IOException {
        HTableDescriptor descriptor = table.getTableDescriptor();
        Utils.deleteTable(Utils.bytes2str(table.getTableName()));
        Utils.createTable(descriptor);
    }
}
