package task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import main.HBShell;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

import utils.ResultLog;
import utils.Utils;

public abstract class TaskBase implements Task {
    public enum TaskType {
        LIST,
        SCAN,
        FILTER,
        PUT,
        DELETE,
        CREATE,
        CLEAR,
        DESCRIBE,
        EXIT,
        QUIT,
        HELP,
    }

    public enum TravelLevel {
        TABLE,
        ROW,
        FAMILY,
        QUALIFIER,
        VALUE,
    }

    protected static final ResultLog log = ResultLog.getLog();

    protected Map<String, Object> patternMap = null;

    private boolean printedTableName  = false;
    private boolean printedRowKey     = false;
    private boolean printedFamilyName = false;

    protected HTable       currentTable                 = null;
    protected List<String> currentFamilies              = new ArrayList<String>();
    protected Result       currentRowFirstKeyOnlyResult = null;

    private static final String[] fBlockFamilyOtherQualifiers = {
        "endtime",
        "index",
        "md5",
        "size",
        "starttime",
        "struct"
    };

    private enum QualifierStatus {
        BEFORE_F_BLOCK,
        F_BLOCK,
        AFTER_F_BLOCK,
    }

    private QualifierStatus qualifierStatus = null;
    private Long   firstFBlockIndex         = null;
    private String firstFBlockOutput        = null;
    private Long   lastFBlockIndex          = null;
    private String lastFBlockOutput         = null;
    private String curFBlockFamilyName      = null;

    private String lastRow    = null;
    private String lastFamily = null;

    protected TaskBase(Map<String, Object> patternMap) {
        this.patternMap = patternMap;
    }

    protected void travelDatabase()
    throws MasterNotRunningException, IOException {
        HTableDescriptor[] hTableDescriptors = Utils.listTables();

        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            String tableName = hTableDescriptor.getNameAsString();

            if (isMatch(HBShell.TABLE_NAME, tableName)) {
                currentFamilies.clear();

                for (byte[] bFamily : hTableDescriptor.getFamiliesKeys()) {
                    currentFamilies.add(Utils.bytes2str(bFamily));
                }

                travelTable(tableName);
            }
        }
    }

    private void travelTable(String tableName)
    throws IOException {
        printedTableName = false;

        if (HBShell.travelLevel == TravelLevel.TABLE) {
            travelRow(tableName, null);
            return;
        }

        Scan scan = new Scan();
        scan.setFilter(new FirstKeyOnlyFilter());

        currentTable = Utils.getTable(tableName);
        ResultScanner resultScanner = currentTable.getScanner(scan);

        for (Result result : resultScanner) {
            currentRowFirstKeyOnlyResult = result;

            String rowKey = Utils.resultGetRowKey(result);

            if (isMatch(HBShell.ROW_KEY, rowKey)) {
                travelRow(tableName, rowKey);
            }
        }

        currentTable.close();
        currentTable = null;
    }

    private void travelRow(String tableName, String rowKey)
    throws IOException {
        printedRowKey = false;

        if (HBShell.travelLevel == TravelLevel.ROW || rowKey == null) {
            travelFamily(tableName, rowKey, null, null);
            return;
        }

        travelRowNonFBlockFamilies(tableName, rowKey);
        travelRowFBlockFamilies(tableName, rowKey);
    }

    // non fBlock families
    private void travelRowNonFBlockFamilies(String tableName, String rowKey)
    throws IOException {
        Get get = new Get(rowKey.getBytes());

        for (String familyName : currentFamilies) {
            if (isMatch(HBShell.FAMILY_NAME, familyName) && !isFBlockFamily(familyName)) {
                get.addFamily(familyName.getBytes());
            }
        }

        if (get.getFamilyMap().isEmpty()) {
            return;
        }

        Result result = currentTable.get(get);

        if (!result.isEmpty()) {
            NavigableMap<byte[], NavigableMap<byte[], byte[]> > familyMap = result.getNoVersionMap();

            for (byte[] bFamilyName : familyMap.keySet()) {
                String familyName = Utils.bytes2str(bFamilyName);
                travelFamily(tableName, rowKey, familyName, familyMap.get(bFamilyName));
            }
        }
    }

    // fBlock families
    private void travelRowFBlockFamilies(String tableName, String rowKey)
    throws IOException {
        if (!HBShell.travelRowFBlockFamilies) {
            return;
        }

        for (String familyName : currentFamilies) {
            if (isMatch(HBShell.FAMILY_NAME, familyName) && isFBlockFamily(familyName)) {
                travelRowFBlockFamiliesOtherQualifier(tableName, rowKey, familyName);
                travelRowFBlockFamiliesFBlockQualifier(tableName, rowKey, familyName);
            }
        }
    }

    private void travelRowFBlockFamiliesOtherQualifier(String tableName, String rowKey, String familyName)
    throws IOException {
        Get get = new Get(rowKey.getBytes());

        for (String qualifierName : fBlockFamilyOtherQualifiers) {
            get.addColumn(familyName.getBytes(), qualifierName.getBytes());
        }

        Result result = currentTable.get(get);

        if (!result.isEmpty()) {
            travelFamily(tableName, rowKey, familyName, result.getFamilyMap(familyName.getBytes()));
        }
    }

    private void travelRowFBlockFamiliesFBlockQualifier(String tableName, String rowKey, String familyName)
    throws IOException {
        boolean extraRow = false;

        // f0 - f399
        for (int i = 0;; i++) {
            Get get = new Get(rowKey.getBytes());
            get.addColumn(familyName.getBytes(), ("f" + i).getBytes());
            Result result = currentTable.get(get);

            if (result.isEmpty()) {
                if (i == 0) {
                    extraRow = true;
                }

                break;
            }

            travelFamily(tableName, rowKey, familyName, result.getFamilyMap(familyName.getBytes()));
        }

        if (extraRow) {
            // f400, f800, ...
            NavigableMap<byte[], byte[]> qualifierMap = currentRowFirstKeyOnlyResult.getFamilyMap(familyName.getBytes());

            if (qualifierMap.isEmpty()) {
                return;
            }

            travelFamily(tableName, rowKey, familyName, qualifierMap);

            // f401, f402, ...
            // f801, f802, ...
            int firstFBlockIndex = 0;

            for (byte[] bQualifierName : qualifierMap.keySet()) {
                String qualifierName = Utils.bytes2str(bQualifierName);
                firstFBlockIndex = Integer.valueOf(qualifierName.substring(1));
            }

            for (int i = firstFBlockIndex + 1;; i++) {
                Get get = new Get(rowKey.getBytes());
                get.addColumn(familyName.getBytes(), ("f" + i).getBytes());
                Result result = currentTable.get(get);

                if (result.isEmpty()) {
                    break;
                }

                travelFamily(tableName, rowKey, familyName, result.getFamilyMap(familyName.getBytes()));
            }
        }
    }

    private void travelFamily(String tableName, String rowKey, String familyName, NavigableMap<byte[], byte[]> qualifierMap)
    throws IOException {
        if (lastRow != rowKey || lastFamily != familyName) {
            // new family start
            printedFamilyName = false;

            if (firstFBlockOutput != null) {
                outputFBlocks();
            }

            resetFBlockSearch();
        }

        if (HBShell.travelLevel == TravelLevel.FAMILY || qualifierMap == null) {
            travelQualifier(tableName, rowKey, familyName, null, null);
            return;
        }

        for (byte[] bQualifierName : qualifierMap.keySet()) {
            String qualifierName = Utils.bytes2str(bQualifierName);

            if (isMatch(HBShell.QUALIFIER_NAME, qualifierName)) {
                travelQualifier(tableName, rowKey, familyName, qualifierName, qualifierMap.get(bQualifierName));

                lastRow    = rowKey;
                lastFamily = familyName;
            }
        }
    }

    private void travelQualifier(String tableName, String rowKey, String familyName, String qualifierName, byte[] bValue)
    throws IOException {
        if (HBShell.travelLevel == TravelLevel.QUALIFIER || bValue == null) {
            travelValue(tableName, rowKey, familyName, qualifierName, null);
            return;
        }

        String value = null;

        if (Utils.isPrintableData(bValue, HBShell.maxPrintableDetectCnt)) {
            int length = (int) Math.min(HBShell.maxPrintableDetectCnt, bValue.length);
            value = Utils.bytes2str(bValue, 0, length);

            if (bValue.length > HBShell.maxPrintableDetectCnt) {
                value += " ...";
            }
        } else {
            value = Utils.getHexStringBase(bValue, HBShell.maxHexStringLength.intValue(), true);
        }

        if (isMatch(HBShell.VALUE, value) && isFilterMath(tableName, rowKey, familyName, qualifierName, value)) {
            travelValue(tableName, rowKey, familyName, qualifierName, value);
        }
    }

    private void travelValue(String tableName, String rowKey, String familyName, String qualifierName, String value)
    throws IOException {
        if (!printedTableName && HBShell.travelLevel.compareTo(TravelLevel.TABLE) >= 0) {
            printedTableName = true;
            foundTable(tableName);
        }

        if (HBShell.travelLevel == TravelLevel.TABLE || rowKey == null) {
            return;
        }

        if (!printedRowKey && HBShell.travelLevel.compareTo(TravelLevel.ROW) >= 0) {
            printedRowKey = true;
            foundRowKey(tableName, rowKey);
        }

        if (HBShell.travelLevel == TravelLevel.ROW || familyName == null) {
            return;
        }

        if (!printedFamilyName && HBShell.travelLevel.compareTo(TravelLevel.FAMILY) >= 0) {
            printedFamilyName = true;
            foundFamilyName(tableName, rowKey, familyName);
        }

        if (HBShell.travelLevel == TravelLevel.FAMILY || qualifierName == null) {
            return;
        }

        if (HBShell.travelLevel.compareTo(TravelLevel.QUALIFIER) == 0) {
            foundQualifierName(tableName, rowKey, familyName, qualifierName);
        }

        if (HBShell.travelLevel == TravelLevel.QUALIFIER || value == null) {
            return;
        }

        if (HBShell.travelLevel.compareTo(TravelLevel.VALUE) == 0) {
            foundValue(tableName, rowKey, familyName, qualifierName, value);
        }
    }

    protected void foundTable(String tableName)
    throws IOException {
        log.info("T: " + tableName);
    }

    protected void foundRowKey(String tableName, String rowKey)
    throws IOException {
        log.info(" R: " + rowKey);
    }

    protected void foundFamilyName(String tableName, String rowKey, String familyName)
    throws IOException {
        log.info("  F: " + familyName);
    }

    protected void foundQualifierName(String tableName, String rowKey, String familyName, String qualifierName)
    throws IOException {
        log.info("   Q: " + qualifierName);
    }

    protected void foundValue(String tableName, String rowKey, String familyName, String qualifierName, String value)
    throws IOException {
        String output = String.format("   Q: %14s = %s", qualifierName, value);

        switch (qualifierStatus) {
        case BEFORE_F_BLOCK:

            if (isFBlockFamily(familyName) && Utils.isMatch(qualifierName, "^f\\d+$")) {
                searchFBlock(qualifierName, output);
                curFBlockFamilyName = familyName;
                qualifierStatus     = QualifierStatus.F_BLOCK;

                return;
            } else {
                break;
            }

        case F_BLOCK:

            if (familyName.equals(curFBlockFamilyName) && Utils.isMatch(qualifierName, "^f\\d+$")) {
                searchFBlock(qualifierName, output);
                return;
            } else {
                outputFBlocks();
                resetFBlockSearch();
                qualifierStatus = QualifierStatus.AFTER_F_BLOCK;
                break;
            }

        case AFTER_F_BLOCK:
            qualifierStatus = QualifierStatus.BEFORE_F_BLOCK;
            break;

        default:
            break;
        }

        log.info(output);
    }

    private boolean isFBlockFamily(String familyName) {
        return familyName.equals("file") || familyName.equals("tmp");
    }

    private void resetFBlockSearch() {
        qualifierStatus     = QualifierStatus.BEFORE_F_BLOCK;
        firstFBlockIndex    = Long.MAX_VALUE;
        firstFBlockOutput   = null;
        lastFBlockIndex     = Long.MIN_VALUE;
        lastFBlockOutput    = null;
        curFBlockFamilyName = null;
    }

    private void searchFBlock(String qualifierName, String output) {
        Long fBlockIndex = Long.valueOf(qualifierName.substring(1));

        if (fBlockIndex < firstFBlockIndex) {
            firstFBlockIndex  = fBlockIndex;
            firstFBlockOutput = output;
        }

        if (fBlockIndex > lastFBlockIndex) {
            lastFBlockIndex  = fBlockIndex;
            lastFBlockOutput = output;
        }
    }

    private void outputFBlocks()
    throws IOException {
        log.info(firstFBlockOutput);

        if (lastFBlockIndex - firstFBlockIndex > 1) {
            log.info("   ...");
        }

        if (lastFBlockIndex != firstFBlockIndex) {
            log.info(lastFBlockOutput);
        }
    }

    protected boolean isMatch(String patternMapKey, String target) {
        Pattern pattern = (Pattern) patternMap.get(patternMapKey);

        if (pattern == null) {
            return true;
        }

        Matcher matcher = pattern.matcher(target);
        return matcher.find();
    }

    private boolean isFilterMath(String tableName, String rowKey, String familyName, String qualifierName, String value) {
        Pattern pattern = (Pattern) getCommonPattern();

        if (pattern == null) {
            return true;
        }

        return pattern.matcher(tableName).find() ||
               pattern.matcher(rowKey).find() ||
               pattern.matcher(familyName).find() ||
               pattern.matcher(qualifierName).find() ||
               pattern.matcher(value).find();
    }

    private Object getCommonPattern() {
        return patternMap.get(HBShell.COMMON);
    }
}
