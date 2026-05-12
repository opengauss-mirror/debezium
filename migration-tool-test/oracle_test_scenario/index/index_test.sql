-- 1. Single index test table
CREATE TABLE single_index_table (
    id NUMBER PRIMARY KEY,
    name VARCHAR2(100),
    age NUMBER,
    department VARCHAR2(100)
);

-- Create single index
CREATE INDEX idx_single_index_name ON single_index_table(name);

-- 2. Composite index test table
CREATE TABLE composite_index_table (
    id NUMBER PRIMARY KEY,
    first_name VARCHAR2(50),
    last_name VARCHAR2(50),
    age NUMBER,
    department VARCHAR2(100)
);

-- Create composite index
CREATE INDEX idx_composite_index_name ON composite_index_table(first_name, last_name);

-- 3. Multiple indexes test table
CREATE TABLE multiple_index_table (
    id NUMBER PRIMARY KEY,
    name VARCHAR2(100),
    age NUMBER,
    department VARCHAR2(100),
    hire_date DATE,
    salary NUMBER
);

-- Create multiple indexes
CREATE INDEX idx_multiple_index_name ON multiple_index_table(name);
CREATE INDEX idx_multiple_index_department ON multiple_index_table(department);
CREATE INDEX idx_multiple_index_hire_date ON multiple_index_table(hire_date);
CREATE INDEX idx_multiple_index_salary ON multiple_index_table(salary);

CREATE TABLE tb_migrate_case040 (
customer_id NUMBER,
order_status VARCHAR2(10),
order_date DATE
);
CREATE INDEX idx_migrate_case040 ON tb_migrate_case040 (customer_id, order_date DESC);



CREATE TABLE t_normal_reverse (
    id          NUMBER PRIMARY KEY, 
    code        VARCHAR2(10),
    status      VARCHAR2(10),
    create_date DATE,
    amount      NUMBER(12,2),
    description VARCHAR2(200)
);

INSERT INTO t_normal_reverse (id, code, status, create_date, amount, description)
SELECT LEVEL,
       'CODE' || MOD(LEVEL, 100),
       CASE MOD(LEVEL, 5) 
           WHEN 0 THEN 'ACTIVE' 
           WHEN 1 THEN 'INACTIVE' 
           ELSE 'PENDING' 
       END,
       SYSDATE - MOD(LEVEL, 1000),
       DBMS_RANDOM.VALUE(1, 10000),
       'Row ' || LEVEL
FROM   DUAL
CONNECT BY LEVEL <= 1000;

CREATE INDEX idx_reverse ON t_normal_reverse(code) REVERSE;
