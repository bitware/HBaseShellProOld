package tnode;

import java.io.IOException;

import org.apache.hadoop.hbase.HTableDescriptor;

import task.TaskBase;
import task.TaskBase.Level;
import utils.Utils;

public class TNodeDatabase extends TNodeBase {
    public TNodeDatabase(TaskBase task) {
        super(task, null, null, null);
    }

    @Override
    protected String formatString() {
        return null;
    }

    @Override
    protected void travelChildren()
    throws IOException {
        HTableDescriptor[] hTableDescriptors = Utils.listTables();

        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            String tableName = hTableDescriptor.getNameAsString();

            if (task.isMatch(Level.TABLE, tableName)) {
                new TNodeTable(task, this, tableName).handle();
            }
        }
    }
}
