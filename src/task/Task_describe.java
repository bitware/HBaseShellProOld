package task;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.client.HTable;

import tnode.TNodeFamily;
import utils.Utils;

public class Task_describe extends TaskBase {
    @Override
    protected String description() {
        return "describe the named table";
    }

    @Override
    protected String usage() {
        return "describe [table_pattern [family_pattern]]";
    }

    @Override
    public String example() {
        return "describe ^135530186920f18b9049b0a0743e86ac3185887c5d";
    }

    @Override
    protected boolean checkArgNumber(int argNumber) {
        return 0 <= argNumber && argNumber <= 2;
    }

    @Override
    protected void assignParam(String[] args) {
        if (args.length >= 1) {
            levelParam.put(Level.TABLE, Pattern.compile(args[0]));
        }

        if (args.length >= 2) {
            levelParam.put(Level.FAMILY, Pattern.compile(args[1]));
        }
    }

    @Override
    protected Level getLevel() {
        return Level.TABLE;
    }

    @Override
    protected boolean notifyEnabled() {
        return true;
    }

    @Override
    protected void foundTable(HTable table)
    throws IOException {
        List<String> families = Utils.getFamilies(table);

        for (String family : families) {
            if (isMatch(Level.FAMILY, family)) {
                new TNodeFamily(null, null, family, null).output();
            }
        }
    }
}
