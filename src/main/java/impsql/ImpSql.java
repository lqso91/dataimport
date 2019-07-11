package impsql;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class ImpSql {
    public static void main(String[] args){
//        String url = "jdbc:oracle:thin:@192.168.10.93:1521/dbgd";
//        String url = "jdbc:oracle:thin:@192.168.10.90:1521/dbsz";
//        String username = "prod_yx";
//        String password = "prod_yx";

        String url = "jdbc:oracle:thin:@192.168.10.99:1521/orcl";
        String username = "dl_rlt";
        String password = "dl_rlt";

        String tableName = "t_g_tg";
        String filePath = "E:\\work\\大连\\数据\\现场线损数据\\t_g_tg20190615.sql";
        
        Connection connection = null;
        Statement statement = null;

        try{
            LineIterator lineIterator = FileUtils.lineIterator(new File(filePath), "UTF-8");
            DriverManager.getDriver(url);
            connection = DriverManager.getConnection(url, username, password);
            connection.setAutoCommit(false);
            statement = connection.createStatement();

            int count = 0;
            StringBuilder sql = new StringBuilder();
            while (lineIterator.hasNext()){
                try{
                    String line = lineIterator.nextLine();
//                    System.out.println(line);
                    if(line == null || line.isEmpty() || line.contains("commit;")) continue;
                    if(line.endsWith(";")){
                        sql.append(line.replaceAll(";", ""));
                        String s = sql.toString();
                        if(s.startsWith("insert") && s.endsWith(")")){
                                statement.addBatch(s);
                            if(++count % 500 == 0){
                                long start = System.currentTimeMillis();
                                statement.executeBatch();
                                connection.commit();
                                long end = System.currentTimeMillis();
                                System.out.println(tableName + " >>>>>>>>>>>> " + count + "," + (end - start));
                            }
                        }
                        sql = new StringBuilder();
                    }else{
                        sql.append(line);
                    }
                }catch (Exception e){
                    e.printStackTrace();
                    sql = new StringBuilder();
                }
            }
            statement.executeBatch();
            connection.commit();
            System.out.println(">>>>>>>>>>>>= " + count);
            connection.setAutoCommit(true);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            DbUtils.closeQuietly(statement);
            DbUtils.closeQuietly(connection);
        }
    }
}