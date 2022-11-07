package Hbase;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseDDL {
    public static Connection connection = HBaseConnect.connection;

    /**
     * Create Hbase Namespace
     *
     * @param namespace
     */
    public static void createNamespace(String namespace) throws IOException {
        Admin admin = connection.getAdmin();
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(namespace);
//        builder.addConfiguration("user", "yaoxu");

        try {
            admin.createNamespace(builder.build());
        } catch (IOException e) {
            System.out.println("Namespace already exist!");
            e.printStackTrace();
        }

        admin.close();
    }

    /**
     * Verify whether the table exist
     *
     * @param namespace
     * @param tableName
     * @return true / false
     */
    public static boolean isTableExists(String namespace, String tableName) throws IOException {
        Admin admin = connection.getAdmin();

        boolean b = false;
        try {
            b = admin.tableExists(TableName.valueOf(namespace, tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        admin.close();

        return b;
    }

    /**
     * Create table
     *
     * @param namespace
     * @param tableName
     * @param columnFamilies (could be more than one columnFamily)
     */
    public static void createTable(String namespace, String tableName, String... columnFamilies) throws IOException {
        if (columnFamilies.length == 0) {
            System.out.println("Please provide at least one columnFamily");
            return;
        }

        if (isTableExists(namespace, tableName)) {
            System.out.println("This table has already exist");
            return;
        }

        Admin admin = connection.getAdmin();

        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, tableName));

        for (String columnFamily : columnFamilies) {
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));

            //only get the latest version
            //columnFamilyDescriptorBuilder.setMaxVersions(5);

            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());

            try {
                admin.createTable(tableDescriptorBuilder.build());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Modify table
     *
     * @param namespace
     * @param tableName
     * @param columnFamily
     * @param version
     */
    public static void modifyTable(String namespace, String tableName, String columnFamily, int version) throws IOException {
        if (!isTableExists(namespace, tableName)) {
            System.out.println("The table doesn't exist!");
            return;
        }

        Admin admin = connection.getAdmin();

        try {
            TableDescriptor descriptor = admin.getDescriptor(TableName.valueOf(namespace, tableName));
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(descriptor);
            ColumnFamilyDescriptor columnFamilyDescriptor = descriptor.getColumnFamily(Bytes.toBytes(columnFamily));
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(columnFamilyDescriptor);

            columnFamilyDescriptorBuilder.setMaxVersions(version);
            tableDescriptorBuilder.modifyColumnFamily(columnFamilyDescriptorBuilder.build());

            admin.modifyTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }

        admin.close();
    }

    /**
     * Delete Table
     *
     * @param namespace
     * @param tableName
     * @return true / false
     */
    public static boolean deleteTable(String namespace, String tableName) throws IOException {
        if (!isTableExists(namespace, tableName)) {
            System.out.println("The table doesn't exist, cannot delete!");
            return false;
        }

        Admin admin = connection.getAdmin();

        try {
            TableName tableNameToBeDeleted = TableName.valueOf(namespace, tableName);
            admin.disableTable(tableNameToBeDeleted);
            admin.deleteTable(tableNameToBeDeleted);
        } catch (IOException e) {
            e.printStackTrace();
        }

        admin.close();

        return true;
    }

}
