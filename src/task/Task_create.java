package task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.TableExistsException;

import utils.Utils;

public class Task_create extends TaskBase {
    @Override
    protected String description() {
        return "create table";
    }

    @Override
    protected String usage() {
        return "create table_name family_name1 [family_name2 ...]";
    }

    @Override
    public String example() {
        return "create test_table family1 family2";
    }

    @Override
    protected boolean checkArgNumber(int argNumber) {
        return argNumber >= 2;
    }

    @Override
    protected void assignParam(String[] args) {
        levelParam.put(Level.TABLE, args[0]);

        List<String> families = new ArrayList<String>();

        for (int i = 1; i < args.length; i++) {
            families.add(args[i]);
        }

        levelParam.put(Level.FAMILY, families);
    }

    @Override
    public void execute()
    throws IOException {
        String    tableName = (String) levelParam.get(Level.TABLE);
        List< ? > families  = (List< ? >)levelParam.get(Level.FAMILY);

        try {
            if (Utils.tableExists(tableName)) {
                throw new TableExistsException("Table '" + tableName + "' already exists");
            }

            Utils.createTable(tableName, families);
        } catch (TableExistsException e) {
            // make error clear
            log.error(null, e);
        }
    }
}
