import pandas as pd
import sqlite3
import os

# 1. Configuración de rutas (Path)
# Esto sirve para que el script funcione en cualquier computadora
base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
csv_path = os.path.join(base_path, 'data', 'dataset.csv')
db_path = os.path.join(base_path, 'sql', 'heart_failure.db')

def crear_base_de_datos():
    print("--- Iniciando proceso ETL ---")
    
    # 2. Extract (Extraer)
    if not os.path.exists(csv_path):
        print(f"ERROR ROJO: No encuentro el archivo en {csv_path}")
        print("¿Asegurate de haber pegado el CSV dentro de la carpeta 'data'?")
        return
    
    print(f"Leyendo CSV desde: {csv_path}")
    df = pd.read_csv(csv_path)
    
    # 3. Transform (Transformar)
    # renombrar las columnas para que sea más fácil escribir SQL luego.
    print("Transformando datos...")
    df.rename(columns={
        'age': 'edad',
        'anaemia': 'anemia',
        'creatinine_phosphokinase': 'cpk',
        'diabetes': 'diabetes',
        'ejection_fraction': 'fraccion_eyeccion',
        'high_blood_pressure': 'presion_alta',
        'platelets': 'plaquetas',
        'serum_creatinine': 'creatinina_serica',
        'serum_sodium': 'sodio_serico',
        'sex': 'sexo',
        'smoking': 'fumador',
        'time': 'dias_seguimiento',
        'DEATH_EVENT': 'muerte'
    }, inplace=True)
    
    # 4. Load (Cargar)
    print(f"Creando base de datos SQL en: {db_path}")
    
    # Conectamos a SQLite (creará el archivo si no existe)
    conn = sqlite3.connect(db_path)
    
    # Volcamos el DataFrame a una tabla SQL llamada 'pacientes'
    df.to_sql('pacientes', conn, if_exists='replace', index=False)
    
    conn.close()
    print("--- ¡ÉXITO! Base de datos creada. ---")

if __name__ == "__main__":
    crear_base_de_datos()