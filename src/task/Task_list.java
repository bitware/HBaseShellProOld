package task;

public class Task_list extends TaskBase {
    @Override
    protected String description() {
        return "list database data at a specified level";
    }

    @Override
    protected String usage() {
        return "list [table_pattern [row_pattern [family_pattern [qualifier_pattern [value_pattern]]]]]";
    }

    @Override
    public String example() {
        return "list ^135530186920f18b9049b0a0743e86ac3185887c5d file";
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
}
