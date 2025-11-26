# publish.py

import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# นำเข้าไลบรารีสำหรับ Google Sheets
import gspread
from gspread_dataframe import set_with_dataframe 

# --- 1. การตั้งค่าตัวแปรคงที่ ---
PRODUCTION_TABLE_NAME = 'movie_facts' 
PRODUCTION_SCHEMA_NAME = 'production'
# GOOGLE_SHEET_TITLE = 'Kaggle Data Pipeline Report'  <--- (ไม่ต้องใช้แล้ว)
WORKSHEET_NAME = 'Final Data' 

# 🚨 แทนที่ด้วย File ID ที่คุณคัดลอกมา
GOOGLE_SHEET_ID = '1ZGoqwqq17L2_6ywhCK27-KsPyJ-V0xcgQfjIYblNmpw' 

# 🚨 ใช้ชื่อไฟล์มาตรฐานสำหรับ Service Account (แก้ไขชื่อตัวแปรให้ตรงกับไฟล์ที่คุณใช้)
CREDENTIALS_FILE = 'client_secret.json' 

def run_publication_pipeline():
    # 1. โหลดตัวแปรสภาพแวดล้อมจาก .env
    load_dotenv()
    # ดึงค่าการเชื่อมต่อจาก .env 
    DB_HOST = os.getenv("DB_HOST")
    DB_USER = os.getenv("POSTGRES_USER")
    DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    DB_NAME = os.getenv("POSTGRES_DB")
    DB_PORT = os.getenv("DB_PORT")

    # --- 2. การเชื่อมต่อฐานข้อมูล (ส่วนนี้ไม่มีการเปลี่ยนแปลง) ---
    try:
        conn_string = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(conn_string)

        print(f"--- เริ่มดึงข้อมูลจาก {PRODUCTION_SCHEMA_NAME}.{PRODUCTION_TABLE_NAME} (Host: {DB_HOST}:{DB_PORT}) ---")
        
        sql_query = f"SELECT * FROM {PRODUCTION_SCHEMA_NAME}.{PRODUCTION_TABLE_NAME};"
        final_df = pd.read_sql(sql_query, con=engine)
        
        print(f"ดึงข้อมูลพร้อมเผยแพร่มาได้ {len(final_df)} แถว")

    except Exception as e:
        print(f"!!! Error: ไม่สามารถเชื่อมต่อ DB หรือดึงข้อมูลได้ !!!")
        print(f"สาเหตุ: {e}")
        return 

    # --- 3. การเผยแพร่ไปยัง Google Sheets (ส่วนที่ได้รับการแก้ไข) ---
    if final_df.empty:
        print("!!! ไม่พบข้อมูลในตาราง Production ไม่สามารถเผยแพร่ได้ !!!")
        return

    print(f"--- เริ่มเผยแพร่ข้อมูลไปยัง Google Sheets: {GOOGLE_SHEET_ID} ---")
    
    try:
        # ใช้ gspread.service_account() 
        gc = gspread.service_account(filename=CREDENTIALS_FILE)
        
        # 🚨 แก้ไข: ลบการเรียกใช้ gc.auth.service_account_email
        print(f"เชื่อมต่อสำเร็จด้วย Service Account.") 
        
        # *** 🚨 แก้ไข: ใช้ gc.open_by_key() แทน gc.open() ***
        try:
            # ใช้ File ID ที่แน่นอน เพื่อข้ามปัญหา Naming Mismatch
            spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID) 
            print(f"พบ Spreadsheet ด้วย ID: {GOOGLE_SHEET_ID}")
        except gspread.SpreadsheetNotFound:
            # หากยังไม่พบไฟล์ด้วย ID ให้แจ้ง Error ที่ชัดเจน
            print(f"!!! Error: ไม่พบ Spreadsheet ด้วย ID นี้ ({GOOGLE_SHEET_ID})")
            print("โปรดตรวจสอบ ID และยืนยันว่าได้แชร์สิทธิ์ Editor ให้ Service Account แล้ว")
            return
            
        # เลือกหรือสร้าง Worksheet
        try:
            worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        except gspread.WorksheetNotFound:
            # หากไม่พบ Worksheet ให้สร้าง Worksheet ใหม่ภายใน Spreadsheet ที่เปิดอยู่
            worksheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows="100", cols="20")
            print(f"สร้าง Worksheet ใหม่ชื่อ '{WORKSHEET_NAME}'")
        
        # เขียน DataFrame ลง Sheets
        set_with_dataframe(worksheet, final_df, row=1, col=1, include_index=False, resize=True)
        
        print(f"*** เผยแพร่ข้อมูล {len(final_df)} แถวไปยัง ID: {GOOGLE_SHEET_ID} เสร็จสิ้น ***")
        print(f"ลิงก์ Spreadsheet: {spreadsheet.url}")
        
    except FileNotFoundError:
        print(f"!!! ERROR: ไม่พบไฟล์ {CREDENTIALS_FILE} โปรดตรวจสอบพาธ !!!")
    except Exception as e:
        print(f"เกิดข้อผิดพลาดในการเชื่อมต่อ/เผยแพร่ Google Sheets: {e}")
        print("โปรดตรวจสอบว่าได้แชร์ Spreadsheet ให้กับอีเมล Service Account แล้ว")


if __name__ == '__main__':
    run_publication_pipeline()