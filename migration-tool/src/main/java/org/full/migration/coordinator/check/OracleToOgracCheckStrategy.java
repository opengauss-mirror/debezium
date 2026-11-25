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

package org.full.migration.coordinator.check;

import org.apache.commons.lang3.StringUtils;
import org.full.migration.model.config.GlobalConfig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Parameter check strategy for the scenario of migrating from Oracle to openGauss
 */
public class OracleToOgracCheckStrategy implements ScenarioParamCheckStrategy {
    @Override
    public boolean check(GlobalConfig globalConfig, String taskType) {
        if ("table".equalsIgnoreCase(taskType)) {
            // Oracle to openGauss table migration check
            return checkTableMigrationParams(globalConfig);
        }
        return true;
    }
    
    /**
     * Check table migration parameters for the scenario of migrating from Oracle to openGauss
     * @param globalConfig Global configuration
     * @return Whether the parameters are valid
     */
    private boolean checkTableMigrationParams(GlobalConfig globalConfig) {
        // Check if dataxHome exists
        boolean dataxHomeValid = checkDataxHome(globalConfig);
        
        // Oracle-specific checks
        boolean oracleSpecificValid = checkOracleSpecificParams(globalConfig);
        
        // Check common DataX parameters
        boolean dataxConfigValid = checkDataXCommonParams(globalConfig);
        
        return dataxHomeValid && oracleSpecificValid && dataxConfigValid;
    }
    
    /**
     * Check if the DataX Home directory exists
     * @param globalConfig Global configuration
     * @return Whether the DataX Home directory is valid
     */
    private boolean checkDataxHome(GlobalConfig globalConfig) {
        String dataxHome = globalConfig.getDatax().getDataxHome();
        if (StringUtils.isBlank(dataxHome)) {
            return true;
        }
        
        try {
            Path dataxHomePath = Paths.get(dataxHome);
            if (!Files.exists(dataxHomePath)) {
                return false;
            }
            
            if (!Files.isDirectory(dataxHomePath)) {
                return false;
            }
            
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * 检查Oracle特有的参数
     * @param globalConfig 全局配置
     * @return 是否有效
     */
    private boolean checkOracleSpecificParams(GlobalConfig globalConfig) {
        // 这里可以添加Oracle特有的参数检查
        // 例如：Oracle连接参数、字符集设置等
        return true;
    }
    
    /**
     * 检查DataX公共参数
     * @param globalConfig 全局配置
     * @return 是否有效
     */
    private boolean checkDataXCommonParams(GlobalConfig globalConfig) {
        // 直接返回true，因为配置现在从oracle2ograc-config.yml文件中加载
        return true;
    }
    
    @Override
    public String getSupportedSourceType() {
        return "oracle";
    }
}