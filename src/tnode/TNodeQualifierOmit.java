package tnode;

import java.io.IOException;

import main.HBShell;

import task.TaskBase;
import task.TaskBase.Level;

public class TNodeQualifierOmit extends TNodeBase {
    private static final String NAME = "...";

    public TNodeQualifierOmit(TaskBase task, TNodeFamily parent) {
        super(task, parent, NAME, Level.QUALIFIER);
    }

    @Override
    protected String formatString() {
        return HBShell.format_qualifierOmit;
    }

    @Override
    public void handle()
    throws IOException {
        output();
    }

    @Override
    protected void travelChildren() {
    }
}
