/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.full.migration.translator;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.SQLCommentHint;
import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddConstraint;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLBlockStatement;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLConstraint;
import com.alibaba.druid.sql.ast.statement.SQLCreateDatabaseStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateFunctionStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateProcedureStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateSequenceStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateTriggerStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLDefault;
import com.alibaba.druid.sql.ast.statement.SQLDropIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLGrantStatement;
import com.alibaba.druid.sql.ast.statement.SQLPrivilegeItem;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.sqlserver.ast.stmt.SQLServerSetTransactionIsolationLevelStatement;
import com.alibaba.druid.sql.dialect.sqlserver.visitor.SQLServerOutputVisitor;
import com.alibaba.druid.util.FnvHash.Constants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * SQLServerToOpenGaussOutputVisitor
 *
 * @since 2025-04-18
 */
public class SQLServerToOpenGaussOutputVisitor extends SQLServerOutputVisitor {
    private static final Logger logger = LoggerFactory.getLogger(SQLServerToOpenGaussOutputVisitor.class);
    private static final String ERR = "err";
    private static final HashSet<String> incompatiblePrivilegeSet = new HashSet<>();
    private static final HashSet<String> commonSchemaPrivilegeSet = new HashSet<>();
    private static final HashSet<String> tablePrivilegeSet = new HashSet<>();
    private static final HashSet<String> routinePrivilegeSet = new HashSet<>();
    private static final HashSet<String> reservedwordSet = new HashSet<>();

    static {
        incompatiblePrivilegeSet.add("PROXY");
        incompatiblePrivilegeSet.add("TRIGGER");
        incompatiblePrivilegeSet.add("SHOW VIEW");
        incompatiblePrivilegeSet.add("LOCK TABLES");
        incompatiblePrivilegeSet.add("EVENT");
        incompatiblePrivilegeSet.add("CREATE VIEW");
        incompatiblePrivilegeSet.add("CREATE TEMPORARY TABLES");
        incompatiblePrivilegeSet.add("CREATE ROUTINE");
        incompatiblePrivilegeSet.add("ALTER ROUTINE");
        commonSchemaPrivilegeSet.add("CREATE");
        commonSchemaPrivilegeSet.add("USAGE");
        commonSchemaPrivilegeSet.add("ALTER");
        commonSchemaPrivilegeSet.add("DROP");
        commonSchemaPrivilegeSet.add("ALL");
        commonSchemaPrivilegeSet.add("ALL PRIVILEGES");
        tablePrivilegeSet.add("SELECT");
        tablePrivilegeSet.add("INSERT");
        tablePrivilegeSet.add("UPDATE");
        tablePrivilegeSet.add("DELETE");
        tablePrivilegeSet.add("REFERENCES");
        tablePrivilegeSet.add("ALTER");
        tablePrivilegeSet.add("DROP");
        tablePrivilegeSet.add("INDEX");
        tablePrivilegeSet.add("ALL");
        tablePrivilegeSet.add("ALL PRIVILEGES");
        routinePrivilegeSet.add("EXECUTE");
        routinePrivilegeSet.add("DROP");
        routinePrivilegeSet.add("ALTER");
        routinePrivilegeSet.add("ALL");
        routinePrivilegeSet.add("ALL PRIVILEGES");
        reservedwordSet.add("number");
        reservedwordSet.add("user");
        reservedwordSet.add("for");
        reservedwordSet.add("check");
        reservedwordSet.add("all");
    }

    private final StringBuilder sb = (StringBuilder) appender;

    private final Map<Integer, String> sqlSetQuantifierMap = new HashMap<Integer, String>() {
        {
            put(0, "");
            put(1, isUppCase() ? "ALL" : "all");
            put(2, isUppCase() ? "DISTINCT" : "distinct");
            put(3, isUppCase() ? "UNIQUE" : "unique");
            put(4, "");
        }
    };

    private boolean isColumnCaseSensitive;

    public SQLServerToOpenGaussOutputVisitor(Appendable appender) {
        super((StringBuilder) appender);
    }

    /**
     * SQLServerToOpenGaussOutputVisitor
     *
     * @param appender appender
     * @param isColumnCaseSensitive columnCaseSensitive
     */
    public SQLServerToOpenGaussOutputVisitor(Appendable appender, boolean isColumnCaseSensitive) {
        super((StringBuilder) appender);
        this.isColumnCaseSensitive = isColumnCaseSensitive;
    }

    private void printNotSupportWord(String word) {
        if (sb.charAt(sb.length() - 1) != '\n') {
            print('\n');
        }
        print("-- " + word + "\n");
    }

    private void printUcaseNotSupportWord(String word) {
        if (sb.charAt(sb.length() - 1) != '\n') {
            print('\n');
        }
        printUcase("-- " + word + "\n");
    }

    private static void gaussFeatureNotSupportLog(String feat) {
        logger.warn("openGauss does not support " + feat);
    }

    private static void chameFeatureNotSupportLog(String feat) {
        logger.warn("chameleon does not support " + feat);
    }

    private static void errHandle(SQLObject x) {
        SQLObject root = x;
        while (root.getParent() != null) {
            root = root.getParent();
        }
        root.putAttribute(ERR, "-- " + ERR);
    }

    private static void errHandle(SQLObject x, String errStr) {
        SQLObject root = x;
        if (x instanceof SQLPrivilegeItem) {
            root = ((SQLPrivilegeItem) x).getAction();
            while (root.getParent() != null) {
                root = root.getParent();
            }
        } else {
            while (root.getParent() != null) {
                root = root.getParent();
            }
        }
        root.putAttribute(ERR, "-- " + errStr);
    }

    private SQLObject getRoot(SQLObject child) {
        SQLObject current = child;
        while (current.getParent() != null) {
            current = current.getParent();
        }
        return current;
    }

    private String getTypeAttribute(SQLObject x) {
        SQLObject parent = getRoot(x);
        String attribute = " ";
        if (parent instanceof SQLCreateViewStatement) {
            attribute = ",view name is " + ((SQLCreateViewStatement) parent).getTableSource().getName();
        } else if (parent instanceof SQLCreateTriggerStatement) {
            attribute = ",trigger name is " + ((SQLCreateTriggerStatement) parent).getName();
        } else if (parent instanceof SQLCreateFunctionStatement) {
            attribute = ",function name is " + ((SQLCreateFunctionStatement) parent).getName();
        } else {
            // parent instanceof SQLCreateProcedureStatement
            attribute = ",procedure name is " + ((SQLCreateProcedureStatement) parent).getName();
        }
        return attribute;
    }

    @Override
    public boolean visit(SQLCreateDatabaseStatement x) {
        if (x.getHeadHintsDirect() != null) {
            for (SQLCommentHint hint : x.getHeadHintsDirect()) {
                hint.accept(this);
            }
        }
        if (x.hasBeforeComment()) {
            this.printlnComments(x.getBeforeCommentsDirect());
        }
        printUcase("create database ");
        x.getName().accept(this);
        println();
        return false;
    }

    @Override
    public boolean visit(SQLCreateViewStatement x) {
        printUcase("create ");
        if (x.isOrReplace()) {
            printUcase("or replace ");
        }
        println();
        printUcase("view ");
        x.getTableSource().accept(this);
        List<SQLTableElement> columns = x.getColumns();
        if (!columns.isEmpty()) {
            print0(" (");
            this.indentCount++;
            println();
            for (int i = 0; i < columns.size(); ++i) {
                if (i != 0) {
                    print0(", ");
                    println();
                }
                columns.get(i).accept(this);
            }
            this.indentCount--;
            println();
            print(')');
        }
        println();
        printUcase("as");
        println();
        SQLSelect subQuery = x.getSubQuery();
        if (subQuery != null) {
            subQuery.accept(this);
        }
        SQLBlockStatement script = x.getScript();
        if (script != null) {
            script.accept(this);
        }
        if (x.isWithCheckOption() || x.isWithCascaded() || x.isWithLocal()) {
            println();
            printUcase("with ");
            if (x.isWithCheckOption()) {
                printUcase("check option");
                println();
            }
        }
        return false;
    }

    @Override
    public boolean visit(SQLCreateSequenceStatement x) {
        printCreateSequenceHeader(x);
        printStartWith(x);
        printIncrementBy(x);
        printMinValue(x);
        printNoMinValue(x);
        printMaxValue(x);
        printNoMaxValue(x);
        printCycle(x);
        printCache(x);
        return false;
    }

    private void printCreateSequenceHeader(SQLCreateSequenceStatement x) {
        this.print0(this.ucase ? "CREATE " : "create ");
        this.print0(this.ucase ? "SEQUENCE " : "sequence ");
        x.getName().accept(this);
        println();
    }

    private void printStartWith(SQLCreateSequenceStatement x) {
        if (x.getStartWith() != null) {
            this.print0(this.ucase ? " START " : " start ");
            x.getStartWith().accept(this);
            println();
        }
    }

    private void printIncrementBy(SQLCreateSequenceStatement x) {
        if (x.getIncrementBy() != null) {
            this.print0(this.ucase ? " INCREMENT " : " increment ");
            x.getIncrementBy().accept(this);
            println();
        }
    }

    private void printMinValue(SQLCreateSequenceStatement x) {
        if (x.getMinValue() != null) {
            this.print0(this.ucase ? " MINVALUE " : " minvalue ");
            x.getMinValue().accept(this);
            println();
        }
    }

    private void printNoMinValue(SQLCreateSequenceStatement x) {
        if (x.isNoMinValue()) {
            if (DbType.postgresql == this.dbType) {
                this.print0(this.ucase ? " NO MINVALUE" : " no minvalue");
            } else {
                this.print0(this.ucase ? " NOMINVALUE" : " nominvalue");
            }
            println();
        }
    }

    private void printMaxValue(SQLCreateSequenceStatement x) {
        if (x.getMaxValue() != null) {
            this.print0(this.ucase ? " MAXVALUE " : " maxvalue ");
            x.getMaxValue().accept(this);
            println();
        }
    }

    private void printNoMaxValue(SQLCreateSequenceStatement x) {
        if (x.isNoMaxValue()) {
            if (DbType.postgresql == this.dbType) {
                this.print0(this.ucase ? " NO MAXVALUE" : " no maxvalue");
            } else {
                this.print0(this.ucase ? " NOMAXVALUE" : " nomaxvalue");
            }
            println();
        }
    }

    private void printCycle(SQLCreateSequenceStatement x) {
        if (x.getCycle() != null) {
            if (x.getCycle()) {
                this.print0(this.ucase ? " CYCLE" : " cycle");
            } else if (DbType.postgresql == this.dbType) {
                this.print0(this.ucase ? " NO CYCLE" : " no cycle");
            } else {
                this.print0(this.ucase ? " NOCYCLE" : " nocycle");
            }
            println();
        }
    }

    private void printCache(SQLCreateSequenceStatement x) {
        Boolean hasCache = x.getCache();
        if (hasCache != null) {
            if (hasCache) {
                this.print0(this.ucase ? " CACHE" : " cache");
                SQLExpr cacheValue = x.getCacheValue();
                if (cacheValue != null) {
                    this.print(' ');
                    cacheValue.accept(this);
                }
            } else {
                this.print0(this.ucase ? " NOCACHE" : " nocache");
            }
            println();
        }
    }

    @Override
    public boolean visit(SQLServerSetTransactionIsolationLevelStatement x) {
        this.print0(this.ucase ? "SET TRANSACTION ISOLATION LEVEL " : "set transaction isolation level ");
        if (x.getLevel().equals("READ COMMITTED") || x.getLevel().equals("REPEATABLE READ") || x.getLevel()
            .equals("SERIALIZABLE")) {
            this.print0(x.getLevel());
        } else {
            this.print0(x.getLevel());
            logger.error("openGauss does not support set transaction isolation level " + x.getLevel());
        }
        return false;
    }

    @Override
    public boolean visit(SQLCreateIndexStatement x) {
        printUcase("create ");
        String type = x.getType();
        if (type != null) {
            if (type.equalsIgnoreCase("unique")) {
                printUcase("unique");
            } else if (type.equalsIgnoreCase("nonclustered")) {
                // openGauss不支持nonclustered关键字，但是不加任何修饰的创建的索引就是非聚集索引，所以这里用空串替换掉
                this.print0("");
            } else if (type.equalsIgnoreCase("clustered")) {
                // openGauss不支持指定clustered关键字创建聚集索引
                printUcaseNotSupportWord("clustered");
                gaussFeatureNotSupportLog("clustered index" + getTypeAttribute(x));
            } else {
                printUcaseNotSupportWord(type);
                logger.warn("unrecognized keyword " + type + getTypeAttribute(x));
            }
            this.print(' ');
        }
        printUcase("index ");
        x.getName().accept(this);
        printUcase(" on ");
        x.getTable().accept(this);
        this.print0(" (");
        this.printAndAccept(x.getItems(), ", ");
        this.print(')');
        return false;
    }

    @Override
    public boolean visit(SQLDropIndexStatement x) {
        printUcase("drop index ");
        gaussFeatureNotSupportLog("specifying table name" + " ," + getTypeAttribute(x));
        if (x.isIfExists()) {
            printUcase("if exists ");
        }
        x.getIndexName().accept(this);
        return false;
    }

    @Override
    protected void printCreateTable(SQLCreateTableStatement x, boolean printSelect) {
        this.print0(this.ucase ? "CREATE " : "create ");
        this.print0(this.ucase ? "TABLE " : "table ");
        this.printTableSourceExpr(x.getName());
        this.printTableElements(x.getTableElementList());
        List<SQLTableElement> tableElementList = x.getTableElementList();
        for (SQLTableElement element : tableElementList) {
            if (element instanceof SQLColumnDefinition && ((SQLColumnDefinition) element).getName()
                .toString()
                .equalsIgnoreCase("index")) {
                println(";");
                printUcase("CREATE INDEX ");
                if (((SQLColumnDefinition) element).getDataType().toString() != null) {
                    String text = ((SQLColumnDefinition) element).getDataType().toString();
                    String[] parts = text.split("\\(", 2);
                    print(parts[0]);
                    print(" ");
                    print("ON ");
                    print(x.getName().toString());
                    print("(" + parts[1]);
                }
            }
        }
    }

    @Override
    protected void printTableElements(List<SQLTableElement> tableElementList) {
        int size = tableElementList.size();
        if (size != 0) {
            this.print0(" (");
            ++this.indentCount;
            this.println();
            // Indicates whether this element is printed
            boolean[] noPrints = new boolean[size];
            for (int i = 0; i < size; i++) {
                SQLTableElement element = tableElementList.get(i);
                noPrints[i] = (element instanceof SQLColumnDefinition && ((SQLColumnDefinition) element).getName()
                    .toString()
                    .equalsIgnoreCase("index"));
            }
            // Indicates whether to print commas
            boolean[] noDots = new boolean[size];
            boolean isPrev = true;
            for (int i = size - 1; i > 0; i--) {
                noDots[i] = noPrints[i] && isPrev;
                isPrev = noDots[i];
            }
            for (int i = 0; i < size; ++i) {
                SQLTableElement element = tableElementList.get(i);
                if (!noPrints[i]) {
                    element.accept(this);
                }
                boolean isPrintDot = (i != size - 1 && !noDots[i + 1] && !noPrints[i]);
                if (isPrintDot) {
                    this.print(',');
                }
                if (this.isPrettyFormat() && element.hasAfterComment()) {
                    this.print(' ');
                    this.printlnComment(element.getAfterCommentsDirect());
                }
                if (isPrintDot) {
                    this.println();
                }
            }
            --this.indentCount;
            this.println();
            this.print(')');
        }
    }

    @Override
    protected void printDataType(SQLDataType x) {
        boolean isParameterized = this.parameterized;
        this.parameterized = false;
        SQLDataType dataType = OpenGaussToSqlserverDataTypeTransformUtil.transformOpenGaussToSqlServer(x);
        this.print0(dataType.getName());
        List<SQLExpr> arguments = dataType.getArguments();
        if (!arguments.isEmpty()) {
            this.print('(');
            int i = 0;
            for (int size = arguments.size(); i < size; ++i) {
                if (i != 0) {
                    this.print0(", ");
                }
                this.printExpr((SQLExpr) arguments.get(i), false);
            }
            this.print(')');
        }
        long nameHash = dataType.nameHashCode64();
        if (nameHash == Constants.TIME || nameHash == Constants.TIMESTAMP) {
            printUcase(" without time zone");
        }
        this.parameterized = isParameterized;
    }

    @Override
    public boolean visit(SQLAlterTableStatement x) {
        List<SQLCommentHint> headHints = x.getHeadHintsDirect();
        if (headHints != null) {
            Iterator var3 = headHints.iterator();
            while (var3.hasNext()) {
                SQLCommentHint hint = (SQLCommentHint) var3.next();
                hint.accept(this);
                this.println();
            }
        }
        printUcase("alter ");
        printUcase("table ");
        this.printTableSourceExpr(x.getName());
        ++this.indentCount;
        for (int i = 0; i < x.getItems().size(); ++i) {
            SQLAlterTableItem item = (SQLAlterTableItem) x.getItems().get(i);
            if (i != 0) {
                this.print(',');
            }
            this.println();
            if (item instanceof SQLAlterTableAddConstraint) {
                SQLConstraint constraint = ((SQLAlterTableAddConstraint) item).getConstraint();
                if (((SQLAlterTableAddConstraint) item).isWithNoCheck()) {
                    ((SQLAlterTableAddConstraint) item).setWithNoCheck(false);
                    gaussFeatureNotSupportLog("With NoCheck");
                }
                if (constraint instanceof SQLDefault) {
                    SQLDefault sqlDefaultConstraint = (SQLDefault) constraint;
                    print(
                        "ALTER " + sqlDefaultConstraint.getColumn() + " SET DEFAULT " + sqlDefaultConstraint.getExpr());
                } else {
                    item.accept(this);
                }
            } else {
                item.accept(this);
            }
        }
        if (!x.getTableOptions().isEmpty()) {
            if (!x.getItems().isEmpty()) {
                this.print(',');
            }
            this.println();
        }
        --this.indentCount;
        return false;
    }

    @Override
    public boolean visit(SQLGrantStatement x) {
        this.print0(this.ucase ? "GRANT " : "grant ");
        this.printAndAccept(x.getPrivileges(), ", ");
        this.printGrantOn(x);
        if (x.getUsers() != null) {
            this.print0(this.ucase ? " TO " : " to ");
            this.printAndAccept(x.getUsers(), ",");
        }
        if (x.getWithGrantOption()) {
            this.print0(this.ucase ? " WITH GRANT OPTION" : " with grant option");
        }
        return false;
    }

    @Override
    protected void printGrantOn(SQLGrantStatement x) {
        if (x.getResource() != null) {
            this.print0(this.ucase ? " ON " : " on ");
            if (x.getResourceType() != null) {
                this.print0(x.getResourceType().name());
                this.print0(" ");
            }
            x.getResource().accept(this);
        }
    }
}

