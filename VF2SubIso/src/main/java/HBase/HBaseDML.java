package Hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseDML {
    public static Connection connection = HBaseConnect.connection;

    /**
     * Put Data
     *
     * @param namespace
     * @param tableName
     * @param rowkey
     * @param columnFamily
     * @param columnName
     * @param value
     */
    public static void putCell(String namespace, String tableName, String rowkey, String columnFamily, String columnName, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName),Bytes.toBytes(value));

        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();
    }

    /**
     * Get Data
     *
     * @param namespace
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnName
     */
    public static void getCells(String namespace,String tableName,String rowKey,String columnFamily,String columnName)throws IOException{
        Table table = connection.getTable(TableName.valueOf(namespace,tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));

        try {
            Result result = table.get(get);
            Cell[] cells = result.rawCells();

            for (Cell cell:cells) {
                String value = new String(CellUtil.cloneValue(cell));
                System.out.println(value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();
    }


}
