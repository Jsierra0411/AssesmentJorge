import pandas as pd

#--funcion para retornar metadata de las tablas asociadas

def table_info(table_name):
    if table_name == 'jobs':
        return {
            "cols": ['id','job'],
            "table_name": 'jobs'
        }
    
    if table_name == 'hired_employees':
        return {
            "cols": ['id','name','datetime','deparment_id','job_id'],
            "table_name": 'hired_employees'
        }
    
    if table_name == 'departments':
        return {
            "cols": ['id','department'],
            "table_name": 'departments'
        }

#---funcion para retornar las queries necesarias para los reportes
    
def querys(type): 
    if type == 1:
        return {
            "sql": """SELECT 
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
        }
    
    if type == 2:

        return {
            "sql": """WITH TODO_0_0 AS (

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
        }

#---funcion para validar cantidad de columnas
    
def data_validation(df,cols, filename, tablename):
    
    number_columns = len(cols)
    number_columns_dataframe = len(df.columns)
    if number_columns != number_columns_dataframe:
            return {
                "STATUS": "FAIL",
                "MESSAGE": f"Different number of columns between the file {filename} and the table {tablename}"
            }
    else:
           return {
                "STATUS": "SUCCESS",
                "MESSAGE": ""
            }
