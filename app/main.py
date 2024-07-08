#--librerias

import pandas as pd
import io
from . import models
from fastapi import FastAPI, Response,status,HTTPException, Depends
from fastapi.responses import StreamingResponse
from fastapi.params import Body
from fastapi import FastAPI, File, UploadFile, status
from fastapi.params import Body
from sqlalchemy import text
from sqlalchemy.orm import Session
from .utils import table_info, querys, data_validation
from .connections import engine, get_db


models.Base.metadata.create_all(bind=engine)

app = FastAPI()


@app.get("/get_data_report", status_code=status.HTTP_200_OK)
async def get_data(report: int, db: Session = Depends(get_db)):
    
    if report not in [1, 2]:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail='Incorrect value, must be 1 or 2')
        
    #---logica para obtener la query del reporte solicitado y ejecutarla

    sql = querys(report)['sql']
    sql_query = text(sql)

    with db.connection() as conn:
        report_data = conn.execute(sql_query)

    #---logica para generar el response con el archivo csv del reporte solicitado
            
    report_data_final = pd.DataFrame(report_data.fetchall(), columns=report_data.keys())
    stream = io.StringIO()
    report_data_final.to_csv(stream, index=False)
    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")

    response.headers["Content-Disposition"] = f"attachment; filename=report_endpoint_{report}.csv"
    return response


@app.post("/insert_data_tables", status_code=status.HTTP_201_CREATED)
async def insert_data(file: UploadFile = File(...), table_insert: str = Body(...), db: Session = Depends(get_db)):
    
    #---definimos los nombres que deben tener las tablas

    table_names = ['departments', 'jobs', 'hired_employees']
    
    #---logica para validar si el archivo es un csv y si el nombre de la tabla es el correcto

    if not file.filename.endswith(".csv"):
        raise HTTPException(status.HTTP_400_BAD_REQUEST,detail='The file must be a CSV')
    if table_insert not in table_names:
        raise HTTPException(status.HTTP_400_BAD_REQUEST,detail='Incorrect type, must be jobs,departments or hired_employees')

    #--logica para leer la informacion contenida dentro de los csv

    contents = await file.read()
    csv_stringio = io.StringIO(contents.decode("utf-8"))
    table_metadata = table_info(table_insert)
        
    cols = table_metadata['cols']
    table_name = table_metadata['table_name']
    dataframe = pd.read_csv(csv_stringio, header=None)

    #---logica para validar que la cantidad de columnas del archivo cargado sean las esperadas

    validate_columns = data_validation(dataframe,cols, file.filename, table_name)

    #---logica para realizar la escritura de los datos en su tabla correspondiente en la db

    if validate_columns["STATUS"] == 'SUCCESS':
            dataframe.columns = cols
            dataframe = dataframe.set_index(cols[0])
            print(dataframe.head(10))
            try:
                  dataframe.to_sql(table_name, con=db.connection(), if_exists='append', index=False)
                  db.commit()
            except Exception as ex:  
                raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=f'INSERT_FAIL, error_message: {ex}')
            else:
               return {"status": f'INSERT_SUCCESS: table:{table_name} file: {file.filename}'}               
    else: 
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=validate_columns['MESSAGE'])
            



      
    
    