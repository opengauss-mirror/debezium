CREATE TABLE t_function_index_test (
    id              NUMBER PRIMARY KEY,
    num_col         NUMBER(18,4),
    int_col         INTEGER,
    float_col       FLOAT(5),
    char_col        VARCHAR2(100),
    text_col        VARCHAR2(4000),
    date_col        DATE,
    rowid_col       ROWID,
    nullable_col    NUMBER,
    angle_col       NUMBER,
    rowid_str_col   VARCHAR2(18)
);

INSERT INTO t_function_index_test (id, num_col, int_col, float_col, char_col, text_col, date_col, nullable_col, angle_col, rowid_str_col)
SELECT LEVEL,
       DBMS_RANDOM.VALUE(-10000, 10000),
       TRUNC(DBMS_RANDOM.VALUE(-1000, 1000)),
       DBMS_RANDOM.VALUE(-999.99, 999.99),
       'CODE' || LPAD(LEVEL, 5, '0'),
       'Sample text for row ' || LEVEL || ' with some words here and there.',
       SYSDATE - MOD(LEVEL, 365),
       CASE WHEN MOD(LEVEL, 7) = 0 THEN NULL ELSE DBMS_RANDOM.VALUE(1, 100) END,
       DBMS_RANDOM.VALUE(0, 360),
       NULL
FROM   DUAL
CONNECT BY LEVEL <= 500;
UPDATE t_function_index_test SET rowid_str_col = ROWIDTOCHAR(ROWID);
COMMIT;

CREATE INDEX idx_func_abs ON t_function_index_test(ABS(num_col));
CREATE INDEX idx_func_lower ON t_function_index_test(LOWER(char_col));
CREATE INDEX idx_func_upper ON t_function_index_test(UPPER(char_col));
CREATE INDEX idx_func_trim ON t_function_index_test(TRIM(text_col));
CREATE INDEX idx_func_reverse ON t_function_index_test(REVERSE(char_col));
CREATE INDEX idx_func_substr ON t_function_index_test(SUBSTR(text_col, 1, 20));
CREATE INDEX idx_func_substrb ON t_function_index_test(SUBSTRB(text_col, 1, 20));
CREATE INDEX idx_func_to_char ON t_function_index_test(TO_CHAR(date_col));
CREATE INDEX idx_func_to_date ON t_function_index_test(TO_DATE(TO_CHAR(date_col, 'yyyy-mm-dd'), 'yyyy-mm-dd'));
CREATE INDEX idx_func_to_number ON t_function_index_test(TO_NUMBER(TO_CHAR(num_col)));
CREATE INDEX idx_func_nvl ON t_function_index_test(NVL(nullable_col, 0));
CREATE INDEX idx_func_nvl2 ON t_function_index_test(NVL2(nullable_col, 1, 0));
CREATE INDEX idx_func_decode ON t_function_index_test(DECODE(int_col, 0, 'ZERO', 1, 'ONE', 'OTHER'));
CREATE INDEX idx_func_trunc_num ON t_function_index_test(TRUNC(num_col));
CREATE INDEX idx_func_trunc_date ON t_function_index_test(TRUNC(date_col, 'yyyy'));
CREATE INDEX idx_func_regexp_instr ON t_function_index_test(REGEXP_INSTR(text_col, 'word'));
CREATE INDEX idx_func_regexp_substr ON t_function_index_test(REGEXP_SUBSTR(text_col, '[a-zA-Z]+'));
CREATE INDEX idx_func_chartorowid ON t_function_index_test(CHARTOROWID(rowid_str_col));
