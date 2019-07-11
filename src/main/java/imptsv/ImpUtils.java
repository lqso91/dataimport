package imptsv;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Map;
import java.util.Set;

/**
 * 导入工具类
 *
 * @author luojie
 * @date 2019-4-30
 */
public class ImpUtils {
    /**
     * 构建INSERT SQL语句
     * @param conn 数据库连接
     * @param tableName 表名
     * @param excludeColumns 需要排除的列
     * @param defaultValues 列的默认值 k:列名，v:默认值
     * @param parameterIndexInfo 接收参数索引信息
     * @return
     * @throws SQLException
     */
    public static String buildInsertSql(Connection conn, String tableName, Set<String> excludeColumns,
                                 Map<String, String> defaultValues, Map<Integer, ParameterIndexInfo> parameterIndexInfo){
        if(excludeColumns == null){
            excludeColumns = Sets.newHashSet();
        }
        if(defaultValues == null){
            defaultValues = Maps.newHashMap();
        }
        PreparedStatement preparedStatement = null;
        StringBuilder sql = new StringBuilder();
        StringBuilder insertSql = new StringBuilder(String.format("INSERT INTO %s (", tableName));
        StringBuilder valuesSql = new StringBuilder("VALUES(");

        try{
            preparedStatement = conn.prepareStatement(String.format("select * from %s", tableName));
            ResultSetMetaData metaData = preparedStatement.getMetaData();
            int allColumnCount = metaData.getColumnCount();
            if(allColumnCount < 1 || allColumnCount <= excludeColumns.size()){
                return null;
            }

            for (int i = 1; i <= allColumnCount; i++){
                String columnName = metaData.getColumnName(i);
                String columnClassName = metaData.getColumnClassName(i);
                boolean nullable = metaData.isNullable(i) != 0;
                int columnSize = metaData.getColumnDisplaySize(i);

                if(excludeColumns.contains(columnName)){
                    continue;
                }
                insertSql.append(String.format("%s, ", columnName));
                if(defaultValues.containsKey(columnName)){
                    valuesSql.append(String.format("%s, ", defaultValues.get(columnName)));
                }else{
                    valuesSql.append("?, ");
                    if(parameterIndexInfo != null){
                        parameterIndexInfo.put(parameterIndexInfo.size() + 1,
                                new ParameterIndexInfo(parameterIndexInfo.size() + 1,
                                        columnName, columnClassName, nullable, columnSize));
                    }
                }
            }
        }catch (SQLException e){
            throw new RuntimeException(e.getMessage(), e);
        }finally {
//            JdbcUtils.closeDBResources(null, preparedStatement, conn);
        }

        insertSql.deleteCharAt(insertSql.length() - 2);
        valuesSql.deleteCharAt(valuesSql.length() - 2);

        insertSql.append(")");
        valuesSql.append(")");

        sql.append(insertSql);
        sql.append(valuesSql);

        return sql.toString();
    }

    public static boolean setParameters(PreparedStatement statement,
                                         Map<Integer, ParameterIndexInfo> parameterIndexInfo,
                                        String[] values) {
        for(Map.Entry<Integer, ParameterIndexInfo> e : parameterIndexInfo.entrySet()){
            ParameterIndexInfo pii = e.getValue();
            try {
                statement.setObject(pii.getIndex(), null);
                String val = values[pii.getIndex() - 1];
                if(val == null){
                    if(!pii.isNullable()){
                        return false;
                    }
                    continue;
                }
                if(val.startsWith("\"")){
                    val = val.replaceAll("\"", "");
                }

                if(pii.getColumnClassName().equalsIgnoreCase("java.lang.string")){
                    if(val.length() > pii.getColumnSize()){
                        return false;
                    }
                    statement.setString(pii.getIndex(), val);
                }else if(pii.getColumnClassName().equalsIgnoreCase("java.sql.Timestamp")){
                    java.sql.Date date = string2SqlDate(val);
                    if(date != null){
                        statement.setDate(pii.getIndex(), string2SqlDate(val));
                    }else {
                        if(!pii.isNullable()){
                            return false;
                        }
                        statement.setObject(pii.getIndex(), null);
                    }
                }else if(pii.getColumnClassName().equalsIgnoreCase("java.math.BigDecimal")){
                    if(NumberUtils.isParsable(val)){
                        statement.setBigDecimal(pii.getIndex(), new BigDecimal(val));
                    }else {
                        if(!pii.isNullable()){
                            return false;
                        }
                    }
                }else{
                    statement.setObject(pii.getIndex(), val);
                }
            } catch (Exception e1) {
                System.out.println(e1);
                return false;
            }
        }
        return true;
    }

    public static java.sql.Date string2SqlDate(String val){
        return utilDate2SqlDate(string2utilDate(val));
    }

    public static java.util.Date string2utilDate(String val){
        if(StringUtils.isEmpty(val)){
            return null;
        }
        java.util.Date date = null;
        try{
            date = DateUtils.parse(val);
        }catch (Exception e){
            System.out.println(e);
        }
        return date;
    }

    /**
     * 将java.sql.Date转为java.util.Date
     * @param sqlDate
     * @return
     */
    public static java.util.Date sqlDate2UtilDate(java.sql.Date sqlDate){
        return sqlDate == null ? null : new java.util.Date(sqlDate.getTime());
    }

    /**
     * 将java.util.Date转为java.sql.Date
     * @param utilDate
     * @return
     */
    public static java.sql.Date utilDate2SqlDate(java.util.Date utilDate){
        return utilDate == null ? null : new java.sql.Date(utilDate.getTime());
    }
}