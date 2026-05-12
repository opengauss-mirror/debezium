-- 创建测试表
CREATE TABLE function_test_table (
    id NUMBER PRIMARY KEY,
    department VARCHAR2(50),
    salary NUMBER
);

-- 插入测试数据
INSERT INTO function_test_table (id, department, salary) VALUES (1, 'IT', 5000);
INSERT INTO function_test_table (id, department, salary) VALUES (2, 'IT', 6000);
INSERT INTO function_test_table (id, department, salary) VALUES (3, 'HR', 4000);
INSERT INTO function_test_table (id, department, salary) VALUES (4, 'HR', 4500);
COMMIT;

-- 1. 基本函数
CREATE OR REPLACE FUNCTION basic_function RETURN VARCHAR2 IS
BEGIN
    RETURN 'Hello, World!';
END;
/

-- 2. 带参数函数
CREATE OR REPLACE FUNCTION parameterized_function (
    p_name IN VARCHAR2,
    p_age IN NUMBER
) RETURN VARCHAR2 IS
BEGIN
    RETURN 'Name: ' || p_name || ', Age: ' || p_age;
END;
/

-- 3. 聚合函数
CREATE OR REPLACE FUNCTION aggregate_function (
    p_department IN VARCHAR2
) RETURN NUMBER IS
    v_avg_salary NUMBER;
BEGIN
    SELECT AVG(salary)
    INTO v_avg_salary
    FROM function_test_table
    WHERE department = p_department;
    
    RETURN v_avg_salary;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 0;
END;
/

-- 4. 复杂逻辑函数
CREATE OR REPLACE FUNCTION complex_function (
    p_start_date IN DATE,
    p_end_date IN DATE
) RETURN NUMBER IS
    v_days_between NUMBER;
    v_weekends NUMBER;
    v_working_days NUMBER;
BEGIN
    -- 计算日期差
    v_days_between := TRUNC(p_end_date) - TRUNC(p_start_date) + 1;
    
    -- 计算周末天数
    SELECT COUNT(*)
    INTO v_weekends
    FROM (
        SELECT TRUNC(p_start_date) + LEVEL - 1 AS day
        FROM dual
        CONNECT BY LEVEL <= v_days_between
    )
    WHERE TO_CHAR(day, 'D') IN ('1', '7');
    
    -- 计算工作日天数
    v_working_days := v_days_between - v_weekends;
    
    RETURN v_working_days;
END;
/

-- 查看创建的函数
SELECT object_name FROM user_procedures WHERE object_type = 'FUNCTION';
SELECT * FROM adm_procedures WHERE OWNER = 'WANG';
