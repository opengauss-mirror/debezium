-- Foreign key relationship test script
-- Test scenarios: Single table foreign key, multi-table cascading foreign key, complex foreign key network
-- 1. Single table foreign key test
-- Create parent table
CREATE TABLE parent_table (
    parent_id NUMBER PRIMARY KEY,
    parent_name VARCHAR2(100)
);

-- Create child table (with foreign key)
CREATE TABLE child_table (
    child_id NUMBER PRIMARY KEY,
    child_name VARCHAR2(100),
    parent_id NUMBER,
    CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) REFERENCES parent_table (parent_id)
);

-- 2. Multi-table cascading foreign key test
-- Create department table
CREATE TABLE department_table (
    department_id NUMBER PRIMARY KEY,
    department_name VARCHAR2(100)
);

-- Create employee table (references department table)
CREATE TABLE employee_table (
    employee_id NUMBER PRIMARY KEY,
    employee_name VARCHAR2(100),
    department_id NUMBER,
    CONSTRAINT fk_employee_department FOREIGN KEY (department_id) REFERENCES department_table (department_id)
);

-- Create employee project table (references employee table)
CREATE TABLE employee_project_table (
    project_id NUMBER PRIMARY KEY,
    project_name VARCHAR2(100),
    employee_id NUMBER,
    CONSTRAINT fk_project_employee FOREIGN KEY (employee_id) REFERENCES employee_table (employee_id)
);

-- 3. Complex foreign key network test
-- Create country table
CREATE TABLE country_table (
    country_id NUMBER PRIMARY KEY,
    country_name VARCHAR2(100)
);

-- Create city table (references country table)
CREATE TABLE city_table (
    city_id NUMBER PRIMARY KEY,
    city_name VARCHAR2(100),
    country_id NUMBER,
    CONSTRAINT fk_city_country FOREIGN KEY (country_id) REFERENCES country_table (country_id)
);

-- Create company table (references city table)
CREATE TABLE company_table (
    company_id NUMBER PRIMARY KEY,
    company_name VARCHAR2(100),
    headquarters_city_id NUMBER,
    CONSTRAINT fk_company_city FOREIGN KEY (headquarters_city_id) REFERENCES city_table (city_id)
);

-- Create company department table (references company and city tables)
CREATE TABLE company_department_table (
    dept_id NUMBER PRIMARY KEY,
    dept_name VARCHAR2(100),
    company_id NUMBER,
    location_city_id NUMBER,
    CONSTRAINT fk_dept_company FOREIGN KEY (company_id) REFERENCES company_table (company_id),
    CONSTRAINT fk_dept_city FOREIGN KEY (location_city_id) REFERENCES city_table (city_id)
);

CREATE TABLE tb_migrate_case033_1 (
product_id NUMBER,
warehouse_id NUMBER,
quantity NUMBER,
CONSTRAINT pk_inventory PRIMARY KEY (product_id, warehouse_id)
);

CREATE TABLE tb_migrate_case033_2 (
order_id NUMBER,
line_no NUMBER,
product_id NUMBER,
warehouse_id NUMBER,
qty_ordered NUMBER,
CONSTRAINT fk_item_inventory
FOREIGN KEY (product_id, warehouse_id)
REFERENCES tb_migrate_case033_1(product_id, warehouse_id)
);

-- View foreign key constraints
SELECT 
    a.table_name, 
    a.constraint_name, 
    a.r_constraint_name, 
    b.table_name AS referenced_table
FROM 
    user_constraints a
JOIN 
    user_constraints b ON a.r_constraint_name = b.constraint_name
WHERE 
    a.constraint_type = 'R'
ORDER BY 
    a.table_name, a.constraint_name;
