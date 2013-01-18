package task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Writables;

import utils.Utils;

// see: /home/hadoop/hbase/bin/rename_table.rb
public class Task_rename extends TaskBase {
    @Override
    protected String description() {
        return "rename table in hbase";
    }

    @Override
    protected String usage() {
        return "rename old_table_name new_table_name";
    }

    @Override
    public String example() {
        return "rename test_table test_table2";
    }

    @Override
    protected boolean checkArgNumber(int argNumber) {
        return argNumber == 2;
    }

    @Override
    protected void assignParam(String[] args) {
        List<String> tableNames = new ArrayList<String>();

        for (int i = 0; i < args.length; i++) {
            tableNames.add(args[i]);
        }

        levelParam.put(Level.TABLE, tableNames);
    }

    @Override
    public void execute()
    throws IOException {
        List< ? > tableNames = (List< ? >)levelParam.get(Level.TABLE);

        String oldTableName = (String) tableNames.get(0);
        String newTableName = (String) tableNames.get(1);

        HBaseAdmin hBaseAdmin = new HBaseAdmin(Utils.conf());

        hBaseAdmin.disableTable(oldTableName);
        renameTable(oldTableName, newTableName);
        hBaseAdmin.enableTable(newTableName);
    }

    private void renameTable(String oldTableName, String newTableName)
    throws IOException {
        // Get configuration to use.
        HBaseConfiguration c = Utils.conf();

        // Set hadoop filesystem configuration using the hbase.rootdir.
        // Otherwise, we'll always use localhost though the hbase.rootdir
        // might be pointing at hdfs location.
        c.set("fs.default.name", c.get(HConstants.HBASE_DIR));
        FileSystem fs = FileSystem.get(c);

        // If new table directory does not exit, create it.  Keep going if already
        // exists because maybe we are rerunning script because it failed first
        // time. Otherwise we are overwriting a pre-existing table.
        Path rootDir     = FSUtils.getRootDir(c);
        Path oldTableDir = fs.makeQualified(new Path(rootDir, new Path(oldTableName)));

        isDirExists(fs, oldTableDir);

        Path newTableDir = fs.makeQualified(new Path(rootDir, newTableName));

        if (!fs.exists(newTableDir)) {
            fs.mkdirs(newTableDir);
        }

        // Run through the meta table moving region mentions from old to new table name.
        HTable metaTable = new HTable(c, HConstants.META_TABLE_NAME);

        Scan          scan    = new Scan();
        ResultScanner scanner = metaTable.getScanner(scan);

        for (Result result : scanner) {
            String      rowID  = Bytes.toString(result.getRow());
            HRegionInfo oldHRI = Writables.getHRegionInfo(result.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER));

            if (oldHRI == null) {
                throw new IOException("HRegionInfo is null for " + rowID);
            }

            if (!isTableRegion(oldTableName, oldHRI)) {
                continue;
            }

            Path oldRDir = new Path(oldTableDir, new Path(String.valueOf(oldHRI.getEncodedName())));

            if (!fs.exists(oldRDir)) {
                log.warn(oldRDir.toString() + " does not exist -- region " + oldHRI.getRegionNameAsString());
            } else {
                // Now make a new HRegionInfo to add to .META. for the new region.
                HRegionInfo newHRI  = createHRI(newTableName, oldHRI);
                Path        newRDir = new Path(newTableDir, new Path(String.valueOf(newHRI.getEncodedName())));

                // Move the region in filesystem
                fs.rename(oldRDir, newRDir);

                // Removing old region from meta
                Delete d = new Delete(result.getRow());
                metaTable.delete(d);

                // Create 'new' region
                HRegion newR = new HRegion(rootDir, null, fs, c, newHRI, null);

                // Add new row. NOTE: Presumption is that only one .META. region. If not,
                // need to do the work to figure proper region to add this new region to.
                Put p = new Put(newR.getRegionName());
                p.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER, Writables.getBytes(newR.getRegionInfo()));
                metaTable.put(p);
            }
        }

        scanner.close();
        fs.delete(oldTableDir, true);
    }

    // Passed 'dir' exists and is a directory else exception
    private void isDirExists(FileSystem fs, Path dir)
    throws IOException {
        if (!fs.exists(dir)) {
            throw new IOException("Does not exist: " + dir.toString());
        }

        if (!isDirectory(fs, dir)) {
            throw new IOException("Not a directory: " + dir.toString());
        }
    }

    private boolean isDirectory(FileSystem fs, Path dir)
    throws IOException {
        FileStatus fileStatus = fs.getFileStatus(dir);
        return fileStatus.isDir();
    }

    // Returns true if the region belongs to passed table
    private boolean isTableRegion(String tableName, HRegionInfo hri) {
        return Bytes.equals(hri.getTableDesc().getName(), tableName.getBytes());
    }

    // Create new HRI based off passed 'oldHRI'
    private HRegionInfo createHRI(String tableName, HRegionInfo oldHRI) {
        HTableDescriptor htd    = oldHRI.getTableDesc();
        HTableDescriptor newHtd = new HTableDescriptor(tableName);

        for (HColumnDescriptor family : htd.getFamilies()) {
            newHtd.addFamily(family);
        }

        return new HRegionInfo(newHtd, oldHRI.getStartKey(), oldHRI.getEndKey(), oldHRI.isSplit());
    }
}
