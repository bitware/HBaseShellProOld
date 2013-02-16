package utils;

import java.awt.event.KeyEvent;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import org.apache.hadoop.hbase.ZooKeeperConnectionException;



public class Utils {
    private static final String UTF_8 = "UTF-8";

    //
    // common
    //

    public static StackTraceElement getCallerInfo() {
        StackTraceElement[] trace = Thread.currentThread().getStackTrace();
        return trace[3];
    }

    //
    // os
    //

    public static boolean isLinux() {
        return fileExists("/dev/null");
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

    //
    // bytes
    //

    public static String bytes2str(byte[] bytes)
    throws UnsupportedEncodingException {
        return new String(bytes, UTF_8);
    }

    public static String bytes2str(byte[] bytes, int offset, int length)
    throws UnsupportedEncodingException {
        return new String(bytes, offset, length, UTF_8);
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

    private static final String       HBASE_CONF_FILE      = "./conf/hbase-site.xml";
    private static final int          MAX_VERSIONS         = 1;
    
    private static Configuration m_hBaseConfiguration = null;

    public static Configuration conf() {
        if (m_hBaseConfiguration == null) {
            m_hBaseConfiguration = HBaseConfiguration.create();

            m_hBaseConfiguration.addResource(new Path(HBASE_CONF_FILE));
        }
        return m_hBaseConfiguration;
    }

    public static HTableDescriptor[] listTables()
    throws MasterNotRunningException, IOException {
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
        boolean ret = false;
        try {
            HBaseAdmin hBaseAdmin = new HBaseAdmin(conf());
            ret = hBaseAdmin.tableExists(tableName);
        } catch (MasterNotRunningException e) {  
            e.printStackTrace();              
        } catch (ZooKeeperConnectionException e) {  
            e.printStackTrace();  
        } catch (IOException e) {  
            e.printStackTrace();  
        }
        return ret;
    }

    public static void createTable(String tableName, List< ? > families)
    throws MasterNotRunningException, IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName.getBytes());

        for (Object family : families) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(family.toString());
            columnDescriptor.setMaxVersions(MAX_VERSIONS);

            tableDescriptor.addFamily(columnDescriptor);
        }

        createTable(tableDescriptor);
    }

    public static void createTable(HTableDescriptor tableDescriptor)
    throws IOException, MasterNotRunningException {
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

    public static String resultGetRowKey(Result result)
    throws UnsupportedEncodingException {
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
        delete.deleteColumn(family.getBytes(), qualifier.getBytes());
        hTable.delete(delete);
    }
}
