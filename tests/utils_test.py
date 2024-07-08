import pytest
import pandas as pd
from app.utils import table_info, querys, data_validation

#--pruebas para la función table_info

def test_table_info_jobs():
    result = table_info('jobs')
    expected = {
        "cols": ['id', 'job'],
        "table_name": 'jobs'
    }
    assert result == expected

def test_table_info_hired_employees():
    result = table_info('hired_employees')
    expected = {
        "cols": ['id', 'name', 'datetime', 'deparment_id', 'job_id'],
        "table_name": 'hired_employees'
    }
    assert result == expected

def test_table_info_departments():
    result = table_info('departments')
    expected = {
        "cols": ['id', 'department'],
        "table_name": 'departments'
    }
    assert result == expected

def test_table_info_invalid():
    result = table_info('invalid_table')
    assert result is None

def test_querys_type_1():
    result = querys(1)
    expected_sql = """SELECT 
                         d.department, 
                         j.job,
                         COUNT(CASE WHEN DATE(h.datetime) BETWEEN '2021-01-01' AND '2021-03-31' THEN h.id end) AS Q1,
                         COUNT(CASE WHEN DATE(h.datetime) BETWEEN '2021-04-01' AND '2021-06-30' THEN h.id end) AS Q2,
                         COUNT(CASE WHEN DATE(h.datetime) BETWEEN '2021-07-01' AND '2021-09-30' THEN h.id end) AS Q3,
                         COUNT(CASE WHEN DATE(h.datetime) BETWEEN '2021-10-01' AND '2021-12-31' THEN h.id end) AS Q4
                     FROM public.hired_employees AS h
                         JOIN public.departments AS d
                             ON h.deparment_id = d.id
                             JOIN jobs as j
                                 ON h.job_id = j.id
                     WHERE h.datetime ilike '%2021%'
                     GROUP BY 1,2
                     ORDER BY 1,2"""
    assert normalize_sql(result["sql"]) == normalize_sql(expected_sql)

def normalize_sql(sql):
    return " ".join(sql.split())

def test_querys_type_2():
    result = querys(2)
    expected_sql = """WITH TODO_0_0 AS (

                             SELECT 
                                 d.id,
                                 d.department,
                                 COUNT(h.id) as hired
                             FROM public.hired_employees  AS h
                                 JOIN public.departments AS d
                                     ON h.deparment_id = d.id
                                         JOIN jobs as j
                                             ON h.job_id = j.id
                             WHERE h.datetime ilike '%2021%'
                             GROUP BY 1,2

                             ),
                 TODO_0 AS(
                             SELECT id,department,hired, 
                             (SELECT AVG(hired) FROM TODO_0_0) as promedio_total
                             FROM TODO_0_0
                             )

                 SELECT id,department,hired FROM TODO_0
                 WHERE hired > promedio_total
                 ORDER by hired DESC"""
    assert normalize_sql(result["sql"]) == normalize_sql(expected_sql)

# Pruebas para la función data_validation
def test_data_validation_success():
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['John', 'Jane', 'Doe'],
        'datetime': ['2021-01-01', '2021-02-01', '2021-03-01'],
        'deparment_id': [1, 2, 3],
        'job_id': [1, 2, 3]
    })
    cols = ['id', 'name', 'datetime', 'deparment_id', 'job_id']
    result = data_validation(df, cols, 'test.csv', 'hired_employees')
    expected = {
        "STATUS": "SUCCESS",
        "MESSAGE": ""
    }
    assert result == expected

def test_data_validation_fail():
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['John', 'Jane', 'Doe']
    })
    cols = ['id', 'name', 'datetime', 'deparment_id', 'job_id']
    result = data_validation(df, cols, 'test.csv', 'hired_employees')
    expected = {
        "STATUS": "FAIL",
        "MESSAGE": "Different number of columns between the file test.csv and the table hired_employees"
    }
    assert result == expected
