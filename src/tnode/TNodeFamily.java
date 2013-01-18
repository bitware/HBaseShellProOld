package tnode;

import java.io.IOException;
import java.util.NavigableMap;

import main.HBShell;

import task.TaskBase;
import task.TaskBase.Level;
import utils.Utils;

public class TNodeFamily extends TNodeBase {
    private final NavigableMap<byte[], byte[]> familyMap;

    public TNodeFamily(TaskBase task, TNodeRow parent, String family, NavigableMap<byte[], byte[]> familyMap) {
        super(task, parent, family, Level.FAMILY);

        this.familyMap = familyMap;
    }

    @Override
    protected String formatString() {
        return HBShell.format_family;
    }

    @Override
    protected void travelChildren()
    throws IOException {
        for (byte[] bQualifier : familyMap.keySet()) {
            String qualifier = Utils.bytes2str(bQualifier);
            new TNodeQualifier(task, this, qualifier, familyMap.get(bQualifier)).handle();
        }
    }

    public static boolean isFileDataFamily(String family) {
        return family.equals("file") || family.equals("tmp");
    }
}
