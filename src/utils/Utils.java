package utils;

import java.awt.event.KeyEvent;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

public class Utils {
    private static final String UTF_8 = "UTF-8";

    //
    // common
    //

    public static StackTraceElement getCallerInfo() {
        StackTraceElement[] trace = Thread.currentThread().getStackTrace();
        return trace[3];
    }

    public static String getClassFilePath(Class< ? > refClass) {
        ProtectionDomain pDomain = refClass.getProtectionDomain();
        CodeSource       cSource = pDomain.getCodeSource();
        URL              loc     = cSource.getLocation();

        return loc.getPath();
    }

    public static String join(Object[] objects, String separator) {
        StringBuffer sb = new StringBuffer();

        for (Object object : objects) {
            sb.append(object + separator);
        }

        // delete last separator
        if (sb.length() > separator.length()) {
            sb.delete(sb.length() - separator.length(), sb.length());
        }

        return sb.toString();
    }

    //
    // os
    //

    public static boolean isLinux() {
        return fileExists("/dev/null");
    }

    //
    // path
    //

    public static String getParentPath(String path) {
        File   file       = new File(path);
        String parentPath = file.getParent();

        return unixStylePath(parentPath);
    }

    private static String unixStylePath(String path) {
        // change "\\" -> "/" on windows
        return path.replaceAll("\\\\", "/");
    }

    //
    // file
    //

    public static boolean fileExists(String filePath) {
        File file = new File(filePath);
        return file.exists();
    }

    public static boolean renameFile(String from, String to) {
        File fileFrom = new File(from);
        File fileTo   = new File(to);

        return fileFrom.renameTo(fileTo);
    }

    public static boolean deleteFile(String filePath) {
        File file = new File(filePath);
        return file.delete();
    }

    public static String makePath(String parentPath, String name) {
        File file = new File(parentPath, name);
        return file.getPath();
    }

    public interface FoundLine {
        // return true to end search, false to continue
        boolean foundLine(String line);
    }

    public static void searchFileFromEnd(String fileName, FoundLine foundLine)
    throws IOException {
        File             file       = new File(fileName);
        RandomAccessFile rf         = new RandomAccessFile(file, "r");
        long             fileLength = file.length();
        StringBuilder    sb         = new StringBuilder();
        boolean          endSearch  = false;

        try {
            // read from end
            for (long filePointer = fileLength - 1; filePointer != -1; filePointer--) {
                rf.seek(filePointer);

                int b = rf.readByte();

                if (b == '\r') {
                    continue;
                } else if (b == '\n') {
                    endSearch = foundLine.foundLine(sb.reverse().toString());

                    if (endSearch) {
                        break;
                    }

                    // prepare to collect another line
                    sb = new StringBuilder();
                } else {
                    sb.append((char)b);
                }
            }

            if (!endSearch) {
                // first line of the file
                foundLine.foundLine(sb.reverse().toString());
            }
        } finally {
            rf.close();
        }
    }

    //
    // bytes
    //

    public static String bytes2str(byte[] bytes) {
        try {
            return new String(bytes, UTF_8);
        } catch (UnsupportedEncodingException e) {
            RootLog.getLog().error(null, e);
        }

        return null;
    }

    public static String bytes2str(byte[] bytes, int offset, int length) {
        try {
            return new String(bytes, offset, length, UTF_8);
        } catch (UnsupportedEncodingException e) {
            RootLog.getLog().error(null, e);
        }

        return null;
    }

    public static String getHexStringBase(byte[] bytes, int length, boolean show0x) {
        StringBuffer stringBuffer = new StringBuffer();

        length = Math.min(length, bytes.length);

        for (int i = 0; i < length; i++) {
            String hex = Integer.toHexString(0xFF & bytes[i]);

            if (show0x) {
                stringBuffer.append((i == 0 ? "" : " ") + "0x");
            }

            if (hex.length() == 1) {
                stringBuffer.append('0');
            }

            stringBuffer.append(hex);
        }

        if (length < bytes.length) {
            stringBuffer.append(" ...");
        }

        return stringBuffer.toString();
    }

    private static boolean isPrintableChar(char c) {
        Character.UnicodeBlock block = Character.UnicodeBlock.of(c);
        return (!Character.isISOControl(c)) &&
               c != KeyEvent.CHAR_UNDEFINED &&
               block != null &&
               block != Character.UnicodeBlock.SPECIALS;
    }

    public static boolean isPrintableData(byte[] data, long maxPrintableDetectCnt) {
        for (int i = 0; i < data.length; i++) {
            if (i == maxPrintableDetectCnt) {
                break;
            }

            byte b = data[i];

            if (!isPrintableChar((char) b)) {
                return false;
            }
        }

        return true;
    }

    //
    // regexp
    //

    public static boolean isMatch(String target, String patternString) {
        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(target);
        return matcher.find();
    }

    //
    // hbase
    //

    private static final String       HBASE_CONF_FILE        = "./conf/hbase-site.xml";
    private static final String       HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    private static final int          MAX_VERSIONS           = 1;
    private static HBaseConfiguration m_hBaseConfiguration   = null;

    public static HBaseConfiguration conf() {
        if (m_hBaseConfiguration == null) {
            setDefaultHBaseConfiguration();
        }

        return m_hBaseConfiguration;
    }

    public static String getQuorums() {
        return conf().get(HBASE_ZOOKEEPER_QUORUM);
    }

    public static void setQuorums(String quorums) {
        setDefaultHBaseConfiguration();
        m_hBaseConfiguration.set(HBASE_ZOOKEEPER_QUORUM, quorums);
    }

    private static void setDefaultHBaseConfiguration() {
        m_hBaseConfiguration = new HBaseConfiguration();
        m_hBaseConfiguration.addResource(new Path(HBASE_CONF_FILE));
    }

    public static HTableDescriptor[] listTables()
    throws IOException {
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf());
        return hBaseAdmin.listTables();
    }

    // tableName

    public static HTable getTable(String tableName)
    throws IOException {
        return new HTable(conf(), tableName);
    }

    public static boolean tableExists(String tableName)
    throws MasterNotRunningException {
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf());
        return hBaseAdmin.tableExists(tableName);
    }

    public static void createTable(String tableName, List< ? > families)
    throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName.getBytes());

        for (Object family : families) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(family.toString());
            columnDescriptor.setMaxVersions(MAX_VERSIONS);

            tableDescriptor.addFamily(columnDescriptor);
        }

        createTable(tableDescriptor);
    }

    public static void createTable(HTableDescriptor tableDescriptor)
    throws IOException {
        new HBaseAdmin(conf()).createTable(tableDescriptor);
    }

    public static void deleteTable(String tableName)
    throws IOException {
        RootLog.getLog().info(tableName);

        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf());

        if (hBaseAdmin.tableExists(tableName)) {
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
        }
    }

    // result

    public static String resultGetRowKey(Result result) {
        byte[] bRowKey = result.getRow();
        return bytes2str(bRowKey);
    }

    // table

    public static List<String> getFamilies(HTable hTable)
    throws IOException {
        List<String> families = new ArrayList<String>();

        HTableDescriptor descriptor = hTable.getTableDescriptor();

        for (byte[] bFamily : descriptor.getFamiliesKeys()) {
            families.add(bytes2str(bFamily));
        }

        return families;
    }

    public static void put(HTable hTable, String rowKey, String family, String qualifier, String value)
    throws IOException {
        RootLog.getLog().info(rowKey + "/" + family + ":" + qualifier + " = " + value);

        Put put = new Put(rowKey.getBytes());
        put.add(family.getBytes(), qualifier.getBytes(), value.getBytes());
        hTable.put(put);
    }

    public static void deleteRow(HTable hTable, String rowKey)
    throws IOException {
        RootLog.getLog().info(rowKey);

        Delete delete = new Delete(rowKey.getBytes());
        hTable.delete(delete);
    }

    public static void deleteFamily(HTable hTable, String rowKey, String family)
    throws IOException {
        RootLog.getLog().info(rowKey + "/" + family);

        Delete delete = new Delete(rowKey.getBytes());
        delete.deleteFamily(family.getBytes());
        hTable.delete(delete);
    }

    public static void deleteQualifier(HTable hTable, String rowKey, String family, String qualifier)
    throws IOException {
        RootLog.getLog().info(rowKey + "/" + family + ":" + qualifier);

        Delete delete = new Delete(rowKey.getBytes());
        delete.deleteColumns(family.getBytes(), qualifier.getBytes());
        hTable.delete(delete);
    }
}
