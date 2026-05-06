-- 覆盖：简体、繁体、生僻字、emoji、特殊符号、超长文本、混合字符
CREATE TABLE test_chinese_full (
    id             NUMBER(10) PRIMARY KEY,
    test_type      VARCHAR2(50),         
    name_short     VARCHAR2(100),        
    content_clob   CLOB,                 
    special_char   VARCHAR2(500),        
    create_time    DATE DEFAULT SYSDATE
) TABLESPACE USERS;

COMMENT ON TABLE test_chinese_full IS 'Oracle 中文全场景扩展测试表';
COMMENT ON COLUMN test_chinese_full.id IS '主键ID';
COMMENT ON COLUMN test_chinese_full.test_type IS '测试场景类型';
COMMENT ON COLUMN test_chinese_full.name_short IS '短中文测试';
COMMENT ON COLUMN test_chinese_full.content_clob IS '长文本CLOB测试';
COMMENT ON COLUMN test_chinese_full.special_char IS '特殊符号/Emoji/繁体/生僻字测试';

-- 1. 基础简体中文
INSERT INTO test_chinese_full (id, test_type, name_short, content_clob)
VALUES (1, '基础简体', '张三', 'Oracle数据库中文存储正常测试，简体中文无乱码。');

-- 2. 繁体中文
INSERT INTO test_chinese_full (id, test_type, name_short, content_clob)
VALUES (2, '繁体中文', '資料庫', 'Oracle 繁體中文儲存測試，確認無亂碼問題。');

-- 3. 生僻字（极易乱码）
INSERT INTO test_chinese_full (id, test_type, name_short, content_clob)
VALUES (3, '生僻字', '龘龘靐齉', '包含生僻字：龘、靐、齉、𰻞，Oracle字符集AL32UTF8可正常存储。');

-- 4. 中英文 + 数字混合
INSERT INTO test_chinese_full (id, test_type, name_short, content_clob)
VALUES (4, '中英混合', 'User007_测试', '中文+English+123456混合测试，Oracle完美支持。');

-- 5. 特殊符号（标点、括号、空格、换行）
INSERT INTO test_chinese_full (id, test_type, special_char)
VALUES (5, '特殊符号', '【】《》——……￥%&*@#！');

-- 6. Emoji 表情（AL32UTF8 支持）
INSERT INTO test_chinese_full (id, test_type, special_char)
VALUES (6, 'Emoji表情', '测试😀😎🔥🚀💡中文+Emoji');

-- 7. 超长文本（CLOB 大字段测试）
INSERT INTO test_chinese_full (id, test_type, content_clob)
VALUES (7, '超长文本',
'这是一段超长中文测试文本，用于验证Oracle CLOB字段对大量中文的存储能力。
包含换行、空格、标点符号、简体、数字混合内容。
确保在查询、导出、导入、程序调用时都不会出现乱码、截断、异常。
超长文本测试通过说明Oracle字符集配置完全正确。');

-- 8. 全角 / 半角 混合
INSERT INTO test_chinese_full (id, test_type, special_char)
VALUES (8, '全角半角', 'ａｂｃ１２３测试 全角半角混合');

-- 9. 空值、空格、空白中文
INSERT INTO test_chinese_full (id, test_type, name_short)
VALUES (9, '空值空格', '  中文 前后带空格  ');

-- 10. 拼音 + 中文组合
INSERT INTO test_chinese_full (id, test_type, name_short)
VALUES (10, '拼音中文', 'Oracle (Shu Ju Ku) 测试');

COMMIT;

