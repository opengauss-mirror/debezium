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
    SQL_EXCEPTION(5300, "SQL执行失败", "SQL execution failed");

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
