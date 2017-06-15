package sm.hive.psql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by Sreekanth Mahesala on 1/6/17.
 */
public class PostgresTest {


    public static void main(String[] args) throws Exception {

        Connection pgConRead;

        pgConRead = DriverManager.getConnection("jdbc:postgresql://hawkeyeprod.cdrrtxu2ffi1.us-west-2.rds.amazonaws.com:5432/hawkeyeprod", "heprodmaster", "arielriver");

        Statement pgStmtRead = pgConRead.createStatement();

        ResultSet res;

        res = pgStmtRead.executeQuery("SELECT 0 AS var0, 1 AS var1, 2 AS var2, 3 AS var3, NULL AS var4 ");

        while (res.next()) {
            System.out.println(res.getBoolean(1));
            System.out.println(res.getBoolean(2));
            System.out.println(res.getBoolean(3));
            System.out.println(res.getBoolean(4));
            System.out.println(res.getBoolean(5));
        }
    }

}
