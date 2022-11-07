package Hbase;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseConnect {
    public static Connection connection = null;

    static {
        try {
            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            System.out.println("Connect Error");
            e.printStackTrace();
        }
    }

    public static void closeConnection() throws IOException {
        if (connection != null) {
            connection.close();
        }
    }
}
