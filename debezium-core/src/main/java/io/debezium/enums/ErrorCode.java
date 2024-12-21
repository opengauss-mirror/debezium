/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package io.debezium.enums;

import io.debezium.log4j.AppenderLoader;

import java.util.Locale;

/**
 * error code
 *
 * @since 2024/12/11
 */
public enum ErrorCode {
    UNKNOWN(5000, "未知异常", "Unknown error"),
    INCORRECT_CONFIGURATION(5100, "参数配置错误", "There is an error in the parameter configuration"),
    IO_EXCEPTION(5200, "文件读写异常", "IO exception"),
    SQL_EXCEPTION(5300, "SQL执行失败", "SQL execution failed"),
    DB_CONNECTION_EXCEPTION(5400, "数据库连接异常", "Database exception"),
    BINLOG_PARSE_EXCEPTION(5500, "binlog解析异常", "Binlog parse exception"),
    AVRO_EXCEPTION(5600, "Avro序列化异常", "Avro exception"),
    KAFKA_PRODUCE_EXCEPTION(5700, "kafka生产者异常", "Kafka produce exception"),
    KAFKA_CONSUMER_EXCEPTION(5800, "kafka消费者异常", "Kafka consumer exception"),
    THREAD_INTERRUPTED_EXCEPTION(5900, "线程中断异常", "Thread interrupted exception"),
    MESSAGE_HANDLE_EXCEPTION(6000, "消息处理异常", "Message handle exception"),
    PROGRESS_COMMIT_EXCEPTION(6100, "进度提交异常", "Progress commit exception"),
    BREAKPOINT_MESSAGE_HANDLE_EXCEPTION(6200, "断点信息管理异常", "Breakpoint message handle exception"),
    DATA_CONVERT_EXCEPTION(6200, "数据解析异常", "Data convert exception");

    private final int code;
    private final String causeCn;
    private final String causeEn;

    ErrorCode(int code, String causeCn, String causeEn) {
        this.code = code;
        this.causeCn = causeCn;
        this.causeEn = causeEn;
    }

    public int getCode() {
        return code;
    }

    public String getCauseCn() {
        return causeCn;
    }

    public String getCauseEn() {
        return causeEn;
    }

    @Override
    public String toString() {
        return getErrorPrefix();
    }

    /**
     * get error prefix
     *
     * @return String error prefix
     */
    public String getErrorPrefix() {
        AppenderLoader.loadAppender();
        return String.format(Locale.ENGLISH, "<CODE:%d> ", code);
    }
}
