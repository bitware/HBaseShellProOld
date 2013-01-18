package tnode;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;

import task.TaskBase;
import task.TaskBase.Level;

public class TNodeFamilyFileData extends TNodeFamily {
    private static final long MAX_FBLOCK_COUNT_IN_ONE_ROW = 400;

    private final HTable table;
    private final byte[] firstFileDataBValue;

    private final long minIndex;
    private final long maxIndex;

    private final TNodeFamily familyNode;

    public TNodeFamilyFileData(TaskBase task, TNodeRow parent, String family, HTable table, String firstFileDataQualifier, byte[] firstFileDataBValue, TNodeFamily familyNode) {
        super(task, parent, family, null);

        this.table               = table;
        this.firstFileDataBValue = firstFileDataBValue;

        this.minIndex = fileDataQualifierIndex(firstFileDataQualifier);
        this.maxIndex = minIndex + MAX_FBLOCK_COUNT_IN_ONE_ROW - 1;

        this.familyNode = familyNode;
    }

    @Override
    public void output()
    throws IOException {
        // this file data family has other non-file-data
        if ((familyNode != null) && (familyNode.outputted)) {
            return;
        }

        super.output();
    }

    @Override
    protected void travelChildren()
    throws IOException {
        // find last file data qualifier
        long firstIndex = minIndex;
        long lastIndex  = searchLastFileDataQualifier(minIndex, maxIndex, maxIndex * 2 - minIndex);

        if ((task.levelParam.get(Level.QUALIFIER) == null) && (task.levelParam.get(Level.VALUE) == null) && (task.levelParam.get(Level.OTHER) == null)) {
            // no qualifier and value filter, show only the first and last file data qualifier

            // first file data qualifier
            new TNodeQualifier(task, this, fileDataQualifier(firstIndex), firstFileDataBValue).handle();

            if (lastIndex != firstIndex) {
                if (lastIndex > firstIndex + 1) {
                    new TNodeQualifierOmit(task, this).handle();
                }

                // last file data qualifier
                Get    get                   = new Get(parent.name.getBytes());
                String lastFileDataQualifier = fileDataQualifier(lastIndex);
                get.addColumn(name.getBytes(), lastFileDataQualifier.getBytes());

                Result result = table.get(get);
                byte[] bValue = result.getValue(name.getBytes(), lastFileDataQualifier.getBytes());
                new TNodeQualifier(task, this, lastFileDataQualifier, bValue).handle();
            }
        } else {
            // qualifier or value filter exists, so filter all
            for (long i = firstIndex; i <= lastIndex; i++) {
                Get get = new Get(parent.name.getBytes());

                // set filter list
                FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);

                // filter qualifier
                CompareFilter qualifierPatternFilter = getQualifierPatternFilter();

                if (qualifierPatternFilter != null) {
                    filterList.addFilter(qualifierPatternFilter);
                }

                // filter value
                CompareFilter valuePatternFilter = getValuePatternFilter();

                if (valuePatternFilter != null) {
                    filterList.addFilter(valuePatternFilter);
                }

                get.setFilter(filterList);

                // file data qualifier i only
                String fileDataQualifierI = fileDataQualifier(i);
                get.addColumn(name.getBytes(), fileDataQualifierI.getBytes());

                // get result
                Result result = table.get(get);

                if (!result.isEmpty()) {
                    byte[] bValue = result.getValue(name.getBytes(), fileDataQualifierI.getBytes());
                    new TNodeQualifier(task, this, fileDataQualifierI, bValue).handle();
                }
            }
        }
    }

    // half search the last file data qualifier
    private long searchLastFileDataQualifier(long min, long lastIndex, long max)
    throws IOException {
        Get get = new Get(parent.name.getBytes());
        get.addColumn(name.getBytes(), fileDataQualifier(lastIndex).getBytes());

        if (table.exists(get)) {
            if (lastIndex == maxIndex || lastIndex == max - 1) {
                return lastIndex;           // last file data qualifier(lastIndex) found
            } else {
                return searchLastFileDataQualifier(lastIndex, (lastIndex + max) / 2, max);
            }
        } else {
            if (lastIndex == min + 1) {
                return min;                 // last file data qualifier(min) found
            } else {
                return searchLastFileDataQualifier(min, (min + lastIndex) / 2, lastIndex);
            }
        }
    }

    private static String fileDataQualifier(long index) {
        return "f" + index;
    }

    private long fileDataQualifierIndex(String fileDataQualifier) {
        return Long.valueOf(fileDataQualifier.substring(1));
    }
}
