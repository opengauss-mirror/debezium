/*
 * Copyright 2026 Huawei Technologies Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.full.migration.exception;

/**
 * 错误码枚举
 */
public enum ErrorCode {
    // 配置错误
    CONFIG_FILE_NOT_FOUND("10101", "配置文件不存在"),
    CONFIG_FILE_FORMAT_ERROR("10102", "配置文件格式错误"),
    CONFIG_PARAMETER_INVALID("10103", "配置参数无效"),
    USER_NOT_EXIST("10104", "用户不存在"),
    USER_NOT_PERMISSION("10105", "用户权限不足"),

    // 连接错误
    DRIVER_LOAD_FAILED("20300", "数据库驱动加载失败"),
    CONNECTION_FAILED("20301", "数据库连接失败"),
    CONNECTION_TIMEOUT("20302", "数据库连接超时"),
    CONNECTION_CLOSED("20303", "数据库连接已关闭"),
    
    // 源数据库错误
    METADATA_READ_FAILED("30401", "元数据读取失败"),
    TABLE_STRUCTURE_READ_FAILED("30402", "表结构读取失败"),
    PERMISSION_DENIED("30403", "数据库权限不足"),
    
    // 数据迁移错误
    DATA_MIGRATION_FAILED("30501", "数据迁移失败"),
    DATA_CONVERSION_ERROR("30502", "数据转换错误"),
    DATA_TRUNCATION_ERROR("30503", "数据截断错误"),
    
    // 策略错误
    STRATEGY_NOT_FOUND("40501", "迁移策略不存在"),
    STRATEGY_EXECUTION_FAILED("40502", "迁移策略执行失败"),
    
    // 转换错误
    TRANSLATOR_NOT_FOUND("50601", "SQL转换器不存在"),
    SQL_TRANSLATION_FAILED("50602", "SQL转换失败"),
    
    // 目标数据库错误
    TARGET_DATABASE_NOT_SUPPORT("60700", "不支持目标数据库"),
    TARGET_DATABASE_CONNECTION_FAILED("60701", "目标数据库连接失败"),
    TARGET_DATABASE_WRITE_FAILED("60702", "目标数据库写入失败"),
    TARGET_DATABASE_CONSTRAINT_VIOLATION("60703", "目标数据库约束冲突"),
    TARGET_DATABASE_SPACE_INSUFFICIENT("60704", "目标数据库空间不足"),
    
    // 线程池错误
    THREAD_POOL_INIT_FAILED("70801", "线程池初始化失败"),
    THREAD_POOL_TASK_EXECUTION_FAILED("70802", "线程池任务执行失败"),
    
    // DataX 外部系统错误
    DATAX_TOOLS_INITIALIZATION_ERROR("80900", "DataX工具初始化失败"),
    DATAX_CONFIG_ERROR("80901", "DataX配置错误"),
    DATAX_EXECUTION_FAILED("80902", "DataX执行失败"),

    // 其他错误
    UNKNOWN_ERROR("99999", "未知错误");

    
    private final String code;
    private final String message;
    
    ErrorCode(String code, String message) {
        this.code = code;
        this.message = message;
    }
    
    public String getCode() {
        return code;
    }
    
    public String getMessage() {
        return message;
    }
}