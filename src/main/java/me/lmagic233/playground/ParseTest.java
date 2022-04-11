package me.lmagic233.playground;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.*;
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
            System.out.println("sql的Kind："+sqlNode.getKind()+"===》"+sqlNode.getClass());

            //一个select语句包含from部分、where部分、select部分等，每一部分都表示一个SqlNode
            //包含了各种SqlNode类型：SqlSelect、SqlIdentifier、SqlLiteral等
            //SqlIdentifier表示标识符，例如表名称、字段名；SqlLiteral表示字面常量，一些具体的数字、字符。
            if(SqlKind.SELECT.equals(sqlNode.getKind())){

                SqlSelect sqlSelect = (SqlSelect) sqlNode;
                SqlNode from=sqlSelect.getFrom();
                SqlNode where=sqlSelect.getWhere();
                SqlNodeList selectList=sqlSelect.getSelectList();
                //标识符
                if(SqlKind.IDENTIFIER.equals(from.getKind())){
                    System.out.println(from.toString());
                }

                if(SqlKind.LESS_THAN.equals(where.getKind())){
                    SqlBasicCall sqlBasicCall=(SqlBasicCall)where;
                    for(SqlNode sqlNode1: sqlBasicCall.operands){
                        if(SqlKind.LITERAL.equals(sqlNode1.getKind())){
                            System.out.println(sqlNode1.toString());
                        }
                    }
                }

                selectList.getList().forEach(x->{
                    if(SqlKind.IDENTIFIER.equals(x.getKind())){
                        System.out.println(x.toString());
                    }
                });
            }


            System.out.println("=======================================================");
            sqlParser = SqlParser
                    .create("select sum(amount) FROM orders", config);
            sqlNode = sqlParser.parseStmt();
            if(SqlKind.SELECT.equals(sqlNode.getKind())) {
                SqlSelect sqlSelect = (SqlSelect) sqlNode;
                sqlSelect.getSelectList().getList().forEach(x->{
                    if(SqlKind.SUM.equals(x.getKind())){
                        SqlBasicCall sqlBasicCall=(SqlBasicCall)x;
                        System.out.println(sqlBasicCall.operands[0]);
                    }
                });
                //解析select部分
                sqlSelect.getSelectList().getList().forEach(x->{
                    if(SqlKind.CAST.equals(x.getKind())){
                        SqlBasicCall sqlBasicCall=(SqlBasicCall)x;
                        System.out.println(sqlBasicCall.operands[0]); //amount
                        SqlDataTypeSpec charType=(SqlDataTypeSpec)sqlBasicCall.operands[1];
                        System.out.println(charType.getTypeName()); //CHAR
                    }
                });
            }

        } catch (SqlParseException e) {
            throw new RuntimeException("", e);
        }
    }
}
