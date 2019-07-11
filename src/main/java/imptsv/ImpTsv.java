package imptsv;

import com.google.common.collect.Maps;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Map;

public class ImpTsv {
    public static void main(String[] args) {
        String url = "jdbc:oracle:thin:@192.168.10.90:1521/dbsz";
        String username = "prod_yx";
        String password = "prod_yx";

//        String url = "jdbc:oracle:thin:@192.168.10.99:1521/orcl";
//        String username = "dl_rlt";
//        String password = "dl_rlt";

//        String url = "jdbc:gbase://192.168.10.141:5258/gdb?useCursorFetch=true";
//        String username = "root";
//        String password = "root123";

        String tableName = "fw_kfgdxx";
        String filePath = "E:\\1\\data_0710_1540\\fw_kfgdxx.tsv";
        String splitRegex = "\t";
//        String splitRegex = ",";

        boolean title = true;
        int batchSize = 3000;
        Connection connection = null;
        PreparedStatement statement = null;

        try{
            long start = System.currentTimeMillis();
            LineIterator lineIterator = FileUtils.lineIterator(new File(filePath), "UTF-8");
            File logFile = new File(filePath + "_" + System.currentTimeMillis() + ".log");

            DriverManager.getDriver(url);
            connection = DriverManager.getConnection(url, username, password);
            connection.setAutoCommit(false);

            Map<Integer, ParameterIndexInfo> parameterIndexInfo = Maps.newHashMap();
            String sql = ImpUtils.buildInsertSql(connection, tableName, null, null, parameterIndexInfo);
            statement = connection.prepareStatement(sql);

            int insertCount = 0;
            int errerCount = 0;
            int lineCount = 0;
            String lastLine = "";
            while (lineIterator.hasNext()){
                try{
                    String line = lineIterator.nextLine();
                    lineCount++;

                    if(title){
                        title = false;
                        FileUtils.write(logFile, line + "\n", true);
                        continue;
                    }

                    if(StringUtils.isNotBlank(lastLine)){
                        line = lastLine + line;
                        lastLine = "";
                    }
                    if(StringUtils.isEmpty(line)){
                        continue;
                    }
                    String[] values = line.split(splitRegex, -1);
                    // 字段值内含有换行符
                    if(values.length < parameterIndexInfo.size()){
                        lastLine = line;
                        continue;
                    }
                    // 字段值内含有分隔符
                    if(values.length > parameterIndexInfo.size()){
                        StringBuilder rLine = new StringBuilder();
                        boolean f = false;
                        for(int i = 0; i < values.length; i++){
                            String s = values[i];
                            rLine.append(s);
                            if(s.startsWith("\"") && !s.endsWith("\"")){
                                f = true;
                            }
                            if(s.equals("\"") || (!s.startsWith("\"") && s.endsWith("\""))){
                                f = false;
                            }
                            if(!f){
                                rLine.append(splitRegex);
                            }
                        }
                        String[] tempValues = rLine.toString().split(splitRegex, -1);

                        if(tempValues.length < parameterIndexInfo.size()){
                            System.out.println(lineCount + "-" + (++errerCount) + "-" + values.length + " > " + rLine);
                            FileUtils.write(logFile, rLine + "\n", true);
                            continue;
                        }
                        if(tempValues.length > parameterIndexInfo.size()){
                            values = new String[parameterIndexInfo.size()];
                            for(int j = 0; j < parameterIndexInfo.size(); j++){
                                values[j] = tempValues[j];
                            }
                        }
                    }

                    boolean b = ImpUtils.setParameters(statement, parameterIndexInfo, values);
                    if(!b){
                        System.out.println(lineCount + "-" + (++errerCount) + "-" + values.length + " > " + line);
                        FileUtils.write(logFile, line + "\n", true);
                        continue;
                    }
                    statement.addBatch();
                    if(++insertCount % batchSize == 0){
                        statement.executeBatch();
                        connection.commit();
                        System.out.println(String.format("%s:%d, 总耗时：%d",
                                tableName, insertCount, (System.currentTimeMillis() - start)));
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
            statement.executeBatch();
            connection.commit();
            System.out.println(String.format("%s:%d, 总耗时：%d",
                    tableName, insertCount, (System.currentTimeMillis() - start)));
            connection.setAutoCommit(true);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            DbUtils.closeQuietly(statement);
            DbUtils.closeQuietly(connection);
        }
    }
}
