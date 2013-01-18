package task;

public class Task_scan extends TaskBase {
    @Override
    protected String description() {
        return "scan database data with given filter";
    }

    @Override
    protected String usage() {
        return "scan [table_pattern [row_pattern [family_pattern [qualifier_pattern [value_pattern]]]]]";
    }

    @Override
    public String example() {
        return "scan ^135530186920f18b9049b0a0743e86ac3185887c5d . fileinfo";
    }

    @Override
    protected boolean checkArgNumber(int argNumber) {
        return 0 <= argNumber && argNumber <= 5;
    }

    @Override
    protected Level getLevel() {
        return Level.VALUE;
    }
}
