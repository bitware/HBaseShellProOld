package task;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;

import utils.Utils;

public class Task_delete extends TaskBase {
    @Override
    protected String description() {
        return "delete data in database with given filter\n" +
               "\n" +
               "** WARNING : 'delete'(and its alias) will delete all tables in database\n" +
               "** NOTE    : use 'delete! ...' to force delete";
    }

    @Override
    protected String usage() {
        return "delete [table_pattern [row_pattern [family_pattern [qualifier_pattern [value_pattern]]]]]";
    }

    @Override
    public String example() {
        return "delete ^test_table family1";
    }

    @Override
    protected boolean checkArgNumber(int argNumber) {
        return 0 <= argNumber && argNumber <= 5;
    }

    @Override
    protected Level getLevel() {
        if (levelParam.size() > 0) {
            return Level.values()[levelParam.size() - 1];
        }

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
        if (level == Level.TABLE) {
            Utils.deleteTable(Utils.bytes2str(table.getTableName()));
        }
    }

    @Override
    protected void foundRow(HTable table, String row)
    throws IOException {
        if (level == Level.ROW) {
            Utils.deleteRow(table, row);
        }
    }

    @Override
    protected void foundFamily(HTable table, String row, String family)
    throws IOException {
        if (level == Level.FAMILY) {
            Utils.deleteFamily(table, row, family);
        }
    }

    @Override
    protected void foundQualifier(HTable table, String row, String family, String qualifier)
    throws IOException {
        if (level == Level.QUALIFIER) {
            Utils.deleteQualifier(table, row, family, qualifier);
        }
    }

    @Override
    protected void foundValue(HTable table, String row, String family, String qualifier, String value)
    throws IOException {
        if (level == Level.VALUE) {
            Utils.deleteQualifier(table, row, family, qualifier);
        }
    }
}
