package me.lmagic233.playground.create;

import java.util.Map;
import java.util.Properties;

class TableInfo{
    private String tableName; // 表名称
    private Map<String,String> fieldsInfo; //字段名称->类型
    private Properties props; //表属性
    private boolean isSideTable; //是否为维表

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Map<String, String> getFieldsInfo() {
        return fieldsInfo;
    }

    public void setFieldsInfo(Map<String, String> fieldsInfo) {
        this.fieldsInfo = fieldsInfo;
    }

    public Properties getProps() {
        return props;
    }

    public void setProps(Properties props) {
        this.props = props;
    }

    public boolean isSideTable() {
        return isSideTable;
    }

    public void setSideTable(boolean sideTable) {
        isSideTable = sideTable;
    }
}