package imptsv;

/**
 * SQL参数索引信息
 *
 * @author luojie
 * @date 2019-4-30
 */
public class ParameterIndexInfo {

    /** 索引值 */
    private int index;

    /** 列名 */
    private String columnName;

    /** 列类型的Java类名 */
    private String columnClassName;

    /** 是否可为空 */
    private boolean nullable;

    /** 字符串列大小 */
    private int columnSize;

    public ParameterIndexInfo() {
    }

    public ParameterIndexInfo(int index, String columnName, String columnClassName, boolean nullable, int columnSize) {
        this.index = index;
        this.columnName = columnName;
        this.columnClassName = columnClassName;
        this.nullable = nullable;
        this.columnSize = columnSize;
    }

    public int getColumnSize() {
        return columnSize;
    }

    public void setColumnSize(int columnSize) {
        this.columnSize = columnSize;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnClassName() {
        return columnClassName;
    }

    public void setColumnClassName(String columnClassName) {
        this.columnClassName = columnClassName;
    }
}
