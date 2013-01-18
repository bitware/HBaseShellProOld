package tnode;

import java.io.IOException;

import main.HBShell;

import task.TaskBase;
import task.TaskBase.Level;

public class TNodeQualifier extends TNodeBase {
    private final byte[] bValue;

    public TNodeQualifier(TaskBase task, TNodeFamily parent, String qualifier, byte[] bValue) {
        super(task, parent, qualifier, Level.QUALIFIER);

        this.bValue = bValue;
    }

    @Override
    protected String formatString() {
        return HBShell.format_qualifier;
    }

    @Override
    protected void travelChildren()
    throws IOException {
        new TNodeValue(task, this, bValue).handle();
    }
}
