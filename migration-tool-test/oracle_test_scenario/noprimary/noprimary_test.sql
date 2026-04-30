-- No primary key table test script
CREATE TABLE simple_nopk_table (
    id NUMBER,
    name VARCHAR2(100),
    age NUMBER,
    department VARCHAR2(50),
    hire_date DATE
);

CREATE TABLE unique_nopk_table (
    id NUMBER,
    name VARCHAR2(100),
    email VARCHAR2(255),
    CONSTRAINT unique_email UNIQUE (email)
);

CREATE TABLE composite_unique_nopk_table (
    dept_id NUMBER,
    emp_id NUMBER,
    name VARCHAR2(100),
    CONSTRAINT unique_dept_emp UNIQUE (dept_id, emp_id)
);

CREATE TABLE large_nopk_table (
    record_id NUMBER,
    transaction_date DATE,
    amount NUMBER(12,2),
    description VARCHAR2(200),
    status VARCHAR2(20),
    customer_id NUMBER
);

CREATE TABLE multi_col_nopk_table (
    col1 NUMBER,
    col2 VARCHAR2(50),
    col3 DATE,
    col4 NUMBER(8,2),
    col5 VARCHAR2(100),
    col6 INTEGER
);

CREATE TABLE lob_nopk_table (
    doc_id NUMBER,
    doc_name VARCHAR2(100),
    doc_content CLOB,
    doc_attachment BLOB,
    created_date DATE
);

CREATE TABLE nullable_nopk_table (
    id NUMBER,
    name VARCHAR2(100),
    address VARCHAR2(200),
    phone VARCHAR2(20),
    email VARCHAR2(100)
);

INSERT INTO simple_nopk_table (id, name, age, department, hire_date) VALUES (1, 'John Doe', 30, 'IT', SYSDATE - 365);
INSERT INTO simple_nopk_table (id, name, age, department, hire_date) VALUES (2, 'Jane Smith', 25, 'HR', SYSDATE - 180);
INSERT INTO simple_nopk_table (id, name, age, department, hire_date) VALUES (3, 'Bob Johnson', 35, 'Finance', SYSDATE - 730);
INSERT INTO simple_nopk_table (id, name, age, department, hire_date) VALUES (4, 'Alice Brown', 28, 'Marketing', SYSDATE - 90);
INSERT INTO simple_nopk_table (id, name, age, department, hire_date) VALUES (5, 'Charlie Davis', 40, 'IT', SYSDATE - 1095);

INSERT INTO unique_nopk_table (id, name, email) VALUES (1, 'John Doe', 'john.doe@example.com');
INSERT INTO unique_nopk_table (id, name, email) VALUES (2, 'Jane Smith', 'jane.smith@example.com');
INSERT INTO unique_nopk_table (id, name, email) VALUES (3, 'Bob Johnson', 'bob.johnson@example.com');

INSERT INTO composite_unique_nopk_table (dept_id, emp_id, name) VALUES (101, 1, 'John Doe');
INSERT INTO composite_unique_nopk_table (dept_id, emp_id, name) VALUES (101, 2, 'Jane Smith');
INSERT INTO composite_unique_nopk_table (dept_id, emp_id, name) VALUES (102, 1, 'Bob Johnson');
INSERT INTO composite_unique_nopk_table (dept_id, emp_id, name) VALUES (102, 2, 'Alice Brown');

INSERT INTO large_nopk_table (record_id, transaction_date, amount, description, status, customer_id) VALUES (1, SYSDATE - 7, 100.50, 'Purchase', 'Completed', 1001);
INSERT INTO large_nopk_table (record_id, transaction_date, amount, description, status, customer_id) VALUES (2, SYSDATE - 6, 250.75, 'Payment', 'Completed', 1002);
INSERT INTO large_nopk_table (record_id, transaction_date, amount, description, status, customer_id) VALUES (3, SYSDATE - 5, 75.25, 'Refund', 'Pending', 1001);
INSERT INTO large_nopk_table (record_id, transaction_date, amount, description, status, customer_id) VALUES (4, SYSDATE - 4, 1500.00, 'Order', 'Processing', 1003);
INSERT INTO large_nopk_table (record_id, transaction_date, amount, description, status, customer_id) VALUES (5, SYSDATE - 3, 300.00, 'Subscription', 'Active', 1004);
INSERT INTO large_nopk_table (record_id, transaction_date, amount, description, status, customer_id) VALUES (6, SYSDATE - 2, 450.50, 'Service', 'Completed', 1002);
INSERT INTO large_nopk_table (record_id, transaction_date, amount, description, status, customer_id) VALUES (7, SYSDATE - 1, 80.00, 'Purchase', 'Completed', 1005);
INSERT INTO large_nopk_table (record_id, transaction_date, amount, description, status, customer_id) VALUES (8, SYSDATE, 200.00, 'Payment', 'Completed', 1001);
INSERT INTO large_nopk_table (record_id, transaction_date, amount, description, status, customer_id) VALUES (9, SYSDATE + 1, 1200.00, 'Order', 'Pending', 1003);
INSERT INTO large_nopk_table (record_id, transaction_date, amount, description, status, customer_id) VALUES (10, SYSDATE + 2, 50.00, 'Purchase', 'Processing', 1004);


INSERT INTO multi_col_nopk_table (col1, col2, col3, col4, col5, col6) VALUES (1, 'Value 1', SYSDATE, 100.50, 'Description 1', 100);
INSERT INTO multi_col_nopk_table (col1, col2, col3, col4, col5, col6) VALUES (2, 'Value 2', SYSDATE - 7, 200.75, 'Description 2', 200);
INSERT INTO multi_col_nopk_table (col1, col2, col3, col4, col5, col6) VALUES (3, 'Value 3', SYSDATE - 14, 300.00, 'Description 3', 300);

INSERT INTO lob_nopk_table (doc_id, doc_name, doc_content, created_date) VALUES (1, 'Document 1', 'This is the content of document 1', SYSDATE);
INSERT INTO lob_nopk_table (doc_id, doc_name, doc_content, created_date) VALUES (2, 'Document 2', 'This is the content of document 2', SYSDATE - 1);
INSERT INTO lob_nopk_table (doc_id, doc_name, doc_content, created_date) VALUES (3, 'Document 3', 'This is the content of document 3', SYSDATE - 2);

INSERT INTO nullable_nopk_table (id, name, address, phone, email) VALUES (1, 'John Doe', '123 Main St', '555-1234', 'john.doe@example.com');
INSERT INTO nullable_nopk_table (id, name, address, phone) VALUES (2, 'Jane Smith', '456 Oak Ave', '555-5678');
INSERT INTO nullable_nopk_table (id, name, email) VALUES (3, 'Bob Johnson', 'bob.johnson@example.com');
INSERT INTO nullable_nopk_table (id, name) VALUES (4, 'Alice Brown');

COMMIT;