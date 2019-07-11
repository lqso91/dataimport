package impsql;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class Test2 {
    public static void main(String[] args){
//        String url = "jdbc:oracle:thin:@192.168.10.93:1521/dbgd";
        String url = "jdbc:oracle:thin:@192.168.10.90:1521/dbsz";
        String username = "prod_yx";
        String password = "prod_yx";

        String tableName = "wfrt_task_exec_info";
        String filePath = "E:\\work\\深圳客服工作单数据\\wfrt_task_exec_info.sql";

        BufferedReader bufferedReader = null;
        Connection connection = null;
        Statement statement = null;

        try{
            bufferedReader = new BufferedReader(new FileReader(filePath), 0xA00000);

            DriverManager.getDriver(url);
            connection = DriverManager.getConnection(url, username, password);
            connection.setAutoCommit(false);
            statement = connection.createStatement();

            int count = 0;
            StringBuilder sql = new StringBuilder();
            while (bufferedReader.ready()){
                try{
                    String line = bufferedReader.readLine();
//                    System.out.println(line);
                    if(line == null || line.isEmpty() || line.contains("commit;")) continue;
                    if(line.endsWith(";")){
                        sql.append(line.replaceAll(";", ""));
                        String s = sql.toString();
                        if(s.startsWith("insert") && s.endsWith(")")){
//                            if(count > 69000)
                                statement.addBatch(s);
                            if(++count % 100 == 0){
                                statement.executeBatch();
                                connection.commit();
                                System.out.println(tableName + " >>>>>>>>>>>> " + count);
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
            IOUtils.closeQuietly(bufferedReader);
            DbUtils.closeQuietly(statement);
            DbUtils.closeQuietly(connection);
        }
    }
}