/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.migration;

import java.util.Locale;

/**
 * base migration config
 *
 * @author jianghongbo
 * @since 2025/02/05
 */
public class BaseMigrationConfig {
    /**
     * migration type, full, incremental and object
     */
    public static final String DATA_MIGRATION_TYPE = "migration.type";

    /**
     * is migration view
     */
    public static final String MIGRATION_VIEW = "migration.view";

    /**
     * is migration func
     */
    public static final String MIGRATION_FUNC = "migration.func";

    /**
     * is migration trigger
     */
    public static final String MIGRATION_TRIGGER = "migration.trigger";

    /**
     * is migration procedure
     */
    public static final String MIGRATION_PROCEDURE = "migration.procedure";

    private MigrationType migrationType;
    private Boolean isMigrationView;
    private Boolean isMigrationFunc;
    private Boolean isMigrationTrigger;
    private Boolean isMigrationPorcedure;

    private BaseMigrationConfig(MigrationType migrationType, Boolean isMigrationView, Boolean isMigrationFunc,
                                Boolean isMigrationTrigger, Boolean isMigrationPorcedure) {
        this.migrationType = migrationType;
        this.isMigrationView = isMigrationView;
        this.isMigrationFunc = isMigrationFunc;
        this.isMigrationTrigger = isMigrationTrigger;
        this.isMigrationPorcedure = isMigrationPorcedure;
    }

    /**
     * get
     *
     * @return MigrationType
     */
    public MigrationType getMigrationType() {
        return migrationType;
    }

    /**
     * get
     *
     * @return Boolean
     */
    public Boolean getMigrationView() {
        return isMigrationView;
    }

    /**
     * get
     *
     * @return Boolean
     */
    public Boolean getMigrationFunc() {
        return isMigrationFunc;
    }

    /**
     * get
     *
     * @return Boolean
     */
    public Boolean getMigrationTrigger() {
        return isMigrationTrigger;
    }

    /**
     * get
     *
     * @return Boolean
     */
    public Boolean getMigrationPorcedure() {
        return isMigrationPorcedure;
    }

    /**
     * builder for BaseMigrationConfig
     *
     * @author jianghongbo
     * @since 2025/2/6
     */
    public static final class Builder {
        private MigrationType migrationType = MigrationType.FULL;
        private Boolean isMigrationView = true;
        private Boolean isMigrationFunc = true;
        private Boolean isMigrationTrigger = true;
        private Boolean isMigrationPorcedure = true;

        /**
         * build migrationType
         *
         * @param migrationType String
         * @return Builder
         */
        public Builder migrationType(String migrationType) {
            if (!isNull(migrationType)) {
                this.migrationType = MigrationType.valueOf(migrationType.toUpperCase(Locale.ROOT));
            }
            return this;
        }

        /**
         * build isMigrationView
         *
         * @param isMigrationView String
         * @return Builder
         */
        public Builder isMigrationView(Boolean isMigrationView) {
            if (!isNull(isMigrationView)) {
                this.isMigrationView = isMigrationView;
            }
            return this;
        }

        /**
         * build isMigrationFunc
         *
         * @param isMigrationFunc String
         * @return Builder
         */
        public Builder isMigrationFunc(Boolean isMigrationFunc) {
            if (!isNull(isMigrationFunc)) {
                this.isMigrationFunc = isMigrationFunc;
            }
            return this;
        }

        /**
         * build isMigrationTrigger
         *
         * @param isMigrationTrigger String
         * @return Builder
         */
        public Builder isMigrationTrigger(Boolean isMigrationTrigger) {
            if (!isNull(isMigrationTrigger)) {
                this.isMigrationTrigger = isMigrationTrigger;
            }
            return this;
        }

        /**
         * build isMigrationPorcedure
         *
         * @param isMigrationPorcedure String
         * @return Builder
         */
        public Builder isMigrationPorcedure(Boolean isMigrationPorcedure) {
            if (!isNull(isMigrationPorcedure)) {
                this.isMigrationPorcedure = isMigrationPorcedure;
            }
            return this;
        }

        /**
         * judge null
         *
         * @param obj Object
         * @return Boolean
         */
        public Boolean isNull(Object obj) {
            return obj == null;
        }

        /**
         * build BaseMigrationConfig
         *
         * @return BaseMigrationConfig
         */
        public BaseMigrationConfig build() {
            return new BaseMigrationConfig(this.migrationType, this.isMigrationView,
                    this.isMigrationFunc, this.isMigrationTrigger, this.isMigrationPorcedure);
        }
    }

    /**
     * migration type enum
     *
     * @author jianghongbo
     * @since 2025/2/6
     */
    public enum MigrationType {
        FULL("FULL"),
        // incremental migration only
        INCREMENTAL("INCREMENTAL"),
        // migration object only
        OBJECT("OBJECT");

        private String migrationType;

        MigrationType(String migrationType) {
            this.migrationType = migrationType;
        }

        /**
         * get enum code
         *
         * @return String
         */
        public String code() {
            return migrationType;
        }
    }

    /**
     * message type enum
     *
     * @author jianghongbo
     * @since 2025/2/6
     */
    public enum MessageType {
        METADATA("METADATA"),
        FULLDATA("FULLDATA"),
        INCREMENTALDATA("INCREMENTALDATA"),
        OBJECT("OBJECT"),
        EOF("EOF");

        private String msgType;

        MessageType(String msgType) {
            this.msgType = msgType;
        }

        /**
         * get enum code
         *
         * @return String
         */
        public String code() {
            return msgType;
        }
    }
}
