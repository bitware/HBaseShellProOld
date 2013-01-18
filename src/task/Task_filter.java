package task;

import java.util.regex.Pattern;

public class Task_filter extends TaskBase {
    @Override
    protected String description() {
        return "scan database data with given filter, other_pattern will be applied like grep finally";
    }

    @Override
    protected String usage() {
        return "filter [table_pattern [row_pattern [family_pattern [qualifier_pattern [value_pattern]]]]] other_pattern";
    }

    @Override
    public String example() {
        return "filter ^135530186920f18b9049b0a0743e86ac3185887c5d fullpath";
    }

    @Override
    protected boolean checkArgNumber(int argNumber) {
        return 1 <= argNumber && argNumber <= 6;
    }

    @Override
    protected void assignParam(String[] args) {
        levelParam.put(Level.OTHER, Pattern.compile(args[args.length - 1]));

        String[] args2 = new String[args.length - 1];
        System.arraycopy(args, 0, args2, 0, args.length - 1);    // remove last arg

        super.assignParam(args2);
    }

    @Override
    protected Level getLevel() {
        return Level.VALUE;
    }
}
