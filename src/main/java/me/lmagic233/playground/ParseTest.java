package me.lmagic233.playground;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

public class ParseTest {
    public static void main(String[] args) {

        SqlParser.Config config = SqlParser.configBuilder()
                .setLex(Lex.MYSQL) //使用mysql 语法
                .build();
        //SqlParser 语法解析器
        SqlParser sqlParser = SqlParser
                .create("select id,name,age FROM stu where age<20", config);
        SqlNode sqlNode = null;
        try {
            sqlNode = sqlParser.parseStmt();
        } catch (SqlParseException e) {
            throw new RuntimeException("", e);
        }
    }
}
