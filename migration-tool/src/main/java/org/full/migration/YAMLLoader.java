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

package org.full.migration;

import org.full.migration.model.config.GlobalConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;

/**
 * YAMLLoader
 *
 * @since 2025-04-18
 */
public class YAMLLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(YAMLLoader.class);

    /**
     * Allowed package prefix for YAML deserialization. Only classes under this package can
     * be instantiated via explicit YAML global tags, mitigating the unsafe deserialization
     * risk of arbitrary class instantiation (e.g. CVE-2022-1471).
     */
    private static final String ALLOWED_PACKAGE_PREFIX =
            GlobalConfig.class.getPackageName() + ".";

    /**
     * loadYamlConfig
     *
     * @param path path
     * @return GlobalConfig
     */
    public static Optional<GlobalConfig> loadYamlConfig(String path) {
        try (InputStream stream = Files.newInputStream(Paths.get(path))) {
            Yaml yaml = new Yaml(new SafeConfigConstructor(GlobalConfig.class, new LoaderOptions()));
            GlobalConfig globalConfig = yaml.loadAs(stream, GlobalConfig.class);
            Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
            Set<ConstraintViolation<GlobalConfig>> violations = validator.validate(globalConfig);
            if (!violations.isEmpty()) {
                violations.forEach(v -> LOGGER.error("the param '{}' is error, reason: {}, please check and retry.",
                    v.getPropertyPath(), v.getMessage()));
                return Optional.empty();
            }
            return Optional.of(globalConfig);
        } catch (IOException e) {
            LOGGER.error("fail to parse yml config, error message: {}", e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Restricted Constructor that only allows YAML global tags to reference classes within the
     * allowed config package. Plain bean mappings (root type from {@code loadAs} and nested
     * property types resolved via reflection) are unaffected, so the original loading logic
     * is preserved.
     */
    private static final class SafeConfigConstructor extends Constructor {
        SafeConfigConstructor(Class<?> theRoot, LoaderOptions loadingConfig) {
            super(theRoot, loadingConfig);
        }

        @Override
        protected Class<?> getClassForName(String name) throws ClassNotFoundException {
            if (!name.startsWith(ALLOWED_PACKAGE_PREFIX)) {
                throw new ClassNotFoundException("Unauthorized class for YAML deserialization: " + name);
            }
            return super.getClassForName(name);
        }
    }
}
