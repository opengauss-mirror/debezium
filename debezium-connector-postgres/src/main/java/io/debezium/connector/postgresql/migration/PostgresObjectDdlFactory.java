/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.connector.postgresql.migration;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.HashMap;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.migration.ObjectDdlFactory;
import io.debezium.migration.ObjectEnum;

/**
 * Description: get postgres object ddl factory class
 *
 * @author jianghongbo
 * @since 2024/11/22
 */
public class PostgresObjectDdlFactory implements ObjectDdlFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresObjectDdlFactory.class);
    private static final Map<ObjectEnum, String> getObjectsSqls = new HashMap<ObjectEnum, String>() {
        {
            put(ObjectEnum.VIEW, "select table_schema, table_name as view_name, view_definition "
                    + " from information_schema.views where table_schema = '%s'");
            put(ObjectEnum.FUNCTION, "SELECT n.nspname AS schema_name, p.proname AS function_name, "
                    + " pg_get_functiondef(p.oid) AS function_definition FROM pg_proc p JOIN pg_namespace n"
                    + " ON n.oid = p.pronamespace WHERE n.nspname = '%s'");
            put(ObjectEnum.TRIGGER, "SELECT n.nspname AS schema_name, c.relname AS table_name, "
                    + " t.tgname AS trigger_name, pg_get_triggerdef(t.oid) AS trigger_definition FROM pg_trigger t"
                    + " JOIN pg_class c ON t.tgrelid = c.oid JOIN pg_namespace n ON c.relnamespace = n.oid"
                    + " WHERE n.nspname = '%s'");
        }
    };

    private static final Map<ObjectEnum, String> createObjectsSqls = new HashMap<ObjectEnum, String>() {
        {
            put(ObjectEnum.VIEW, "CREATE OR REPLACE VIEW %s.%s AS %s");
        }
    };

    public PostgresObjectDdlFactory() {
    }

    @Override
    public Map<String, String> generateObjectDdl(String schema, ObjectEnum objType, Connection conn) {
        Map<String, String> objDdl = new HashMap<>();
        String sqlGetDef = String.format(getObjectsSqls.get(objType), schema);
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sqlGetDef)) {
            if (objType.equals(ObjectEnum.VIEW)) {
                objDdl = getViewDdl(rs);
            } else if (objType.equals(ObjectEnum.FUNCTION)) {
                objDdl = getFuncDdl(rs);
            } else if (objType.equals(ObjectEnum.TRIGGER)) {
                objDdl = getTriggerDdl(rs);
            } else {
                throw new IllegalArgumentException("Unsupported object type" + objType.code());
            }
        } catch (SQLException e) {
            LOGGER.error("get object defination occured SQLException", e);
        }
        return objDdl;
    }

    private Map<String, String> getViewDdl(ResultSet rs) throws SQLException {
        Map<String, String> viewDdls = new HashMap<>();
        while (rs.next()) {
            String schemaName = rs.getString(1);
            String viewName = rs.getString(2);
            String viewDefinition = rs.getString(3);
            viewDdls.put(viewName, String.format(Locale.ROOT, createObjectsSqls.get(ObjectEnum.VIEW),
                    schemaName, viewName, viewDefinition));
        }
        return viewDdls;
    }

    private Map<String, String> getFuncDdl(ResultSet rs) throws SQLException {
        Map<String, String> funcDdls = new HashMap<>();
        while (rs.next()) {
            funcDdls.put(rs.getString(2), rs.getString(3));
        }
        return funcDdls;
    }

    private Map<String, String> getTriggerDdl(ResultSet rs) throws SQLException {
        Map<String, String> triggerDdl = new HashMap<>();
        while (rs.next()) {
            triggerDdl.put(rs.getString(3), rs.getString(4));
        }
        return triggerDdl;
    }
}
