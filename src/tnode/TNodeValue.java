package tnode;

import java.io.IOException;

import main.HBShell;

import task.TaskBase;
import task.TaskBase.Level;
import utils.Utils;

public class TNodeValue extends TNodeBase {
    public TNodeValue(TaskBase task, TNodeQualifier parent, byte[] bValue) {
        super(task, parent, valueString(bValue), Level.VALUE);
    }

    @Override
    protected String formatString() {
        return parent.formatString() + HBShell.format_value;
    }

    @Override
    public void output()
    throws IOException {
        if (!otherFilterPassed()) {
            return;
        }

        parent.parent.output();
        log.info(String.format(formatString(), parent.name, name));

        if (task != null) {
            task.notifyFound(this);
        }
    }

    @Override
    protected void travelChildren() {
        // no children
    }

    private static String valueString(byte[] bValue) {
        String value = null;

        if (!Utils.isPrintableData(bValue, HBShell.maxPrintableDetectCnt)) {
            value = Utils.getHexStringBase(bValue, HBShell.maxHexStringLength.intValue(), true);
        } else {
            int length = (int)Math.min(bValue.length, HBShell.maxPrintableDetectCnt);
            value = Utils.bytes2str(bValue, 0, length);

            if (bValue.length > HBShell.maxPrintableDetectCnt) {
                value += " ...";
            }
        }

        return value;
    }
}
