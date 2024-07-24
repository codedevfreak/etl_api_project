import pandas as pd
import sqlite3

def extract_data(csv_file):
    # Ekstraksi data dari file CSV
    return pd.read_csv(csv_file)

def transform_data(df):
    # Normalisasi nama kolom
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    
    # Menangani nilai kosong
    df.fillna('', inplace=True)
    
    # Konversi tipe data
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()
    
    # Contoh: mengonversi kolom tanggal menjadi tipe datetime
    if 'date_column' in df.columns:
        df['date_column'] = pd.to_datetime(df['date_column'], errors='coerce')

    # Menghapus duplikasi
    df.drop_duplicates(inplace=True)
    
    return df

def load_data(df, db_name='data_lake.db'):
    # Membuat koneksi ke database SQLite
    conn = sqlite3.connect(db_name)
    
    # Memuat data ke tabel 'processed_data'
    df.to_sql('processed_data', conn, if_exists='replace', index=False)
    
    # Menutup koneksi
    conn.close()

if __name__ == "__main__":
    # Path ke file CSV
    csv_file = 'C:\\Users\\Administrator\\Documents\\etl_flask_project\\data\\sample_datatest.csv'
    
    # Proses ETL
    data = extract_data(csv_file)
    transformed_data = transform_data(data)
    load_data(transformed_data)
    
    print("Data has been loaded successfully.")
