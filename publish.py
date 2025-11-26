# publish.py

import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import psycopg2

# นำเข้าไลบรารีสำหรับ Google Sheets
import gspread
from gspread_dataframe import set_with_dataframe # ใช้สำหรับเขียน DataFrame ลง Sheets

# --- 1. การตั้งค่าตัวแปรคงที่ ---
PRODUCTION_TABLE_NAME = 'movie_facts' 
PRODUCTION_SCHEMA_NAME = 'production'
GOOGLE_SHEET_TITLE = 'Kaggle Data Pipeline Report' 
WORKSHEET_NAME = 'Final Data' 
CREDENTIALS_FILE = r'D:\AIE321\PJ\AIE321_Bigdata_Movie_KPI_1M\credentials.json' 

def run_publication_pipeline():
    """
    ฟังก์ชันหลักในการดึงข้อมูลจาก Production DB และเผยแพร่ไปยัง Google Sheets
    """
    # 1. โหลดตัวแปรสภาพแวดล้อมจาก .env
    load_dotenv()

    # ดึงค่าการเชื่อมต่อจาก .env (สำคัญ: ใช้ DB_HOST=localhost และ DB_PORT=6666)
    DB_USER = 'DB_AIE321_BIG_DATA'
    DB_PASSWORD = '321bigdatawork'
    DB_HOST = 'localhost' 
    DB_PORT = '6666'      
    DB_NAME = 'AIE321' 

    # --- 2. การเชื่อมต่อฐานข้อมูล ---
    try:
        # 🚨 ใช้ Connection String สำหรับ psycopg2 โดยตรง
        conn_string_psycopg2 = f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT}"
        
        print(f"--- เริ่มดึงข้อมูลจาก production.genre_average_revenue (Host: {DB_HOST}:{DB_PORT}) ---")

        # 🚨 ใช้ psycopg2.connect() เพื่อรับ Connection object ที่มี .cursor() แน่นอน
        with psycopg2.connect(conn_string_psycopg2) as conn:
            # 📌 ใช้ pd.read_sql_query กับ Connection object ของ psycopg2
            sql_query_string = f"SELECT * FROM {PRODUCTION_SCHEMA_NAME}.genre_average_revenue ORDER BY average_revenue DESC;" 
            final_df = pd.read_sql_query(sql_query_string, conn) # <--- ส่ง conn ของ psycopg2 เข้าไป

        print(f"ดึงข้อมูลพร้อมเผยแพร่มาได้ {len(final_df)} แถว")

    except Exception as e:
        print(f"!!! Error: ไม่สามารถเชื่อมต่อ DB หรือดึงข้อมูลได้ !!!")
        print(f"สาเหตุ: {e}")
        # หากดึงข้อมูลไม่ได้ ให้หยุดการทำงาน
        return 

    # --- 3. การเผยแพร่ไปยัง Google Sheets ---
    if final_df.empty:
        print("!!! ไม่พบข้อมูลในตาราง Production ไม่สามารถเผยแพร่ได้ !!!")
        return

    print(f"--- เริ่มเผยแพร่ข้อมูลไปยัง Google Sheets: {GOOGLE_SHEET_TITLE} ---")
    
    try:
        # เชื่อมต่อกับ Google Service Account
        gc = gspread.service_account(filename=CREDENTIALS_FILE)
        
        # เปิด Spreadsheet (หรือสร้างใหม่)
        try:
            spreadsheet = gc.open(GOOGLE_SHEET_TITLE)
        except gspread.SpreadsheetNotFound:
            print(f"ไม่พบ Spreadsheet กำลังสร้างใหม่ชื่อ '{GOOGLE_SHEET_TITLE}'...")
            spreadsheet = gc.create(GOOGLE_SHEET_TITLE)
            # สำคัญ: ต้องแชร์ Spreadsheet นี้ให้กับอีเมล Service Account ด้วยตนเอง 
            
        # เลือกหรือสร้าง Worksheet
        try:
            worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows="100", cols="20")
        
        # เขียน DataFrame ลง Sheets
        set_with_dataframe(worksheet, final_df, row=1, col=1, include_index=False, resize=True)
        
        print(f"*** เผยแพร่ข้อมูล {len(final_df)} แถวไปยัง '{GOOGLE_SHEET_TITLE}' เสร็จสิ้น ***")
        print(f"ลิงก์ Spreadsheet: {spreadsheet.url}")
        
    except FileNotFoundError:
        print(f"!!! ERROR: ไม่พบไฟล์ credentials.json โปรดตรวจสอบพาธ !!!")
    except Exception as e:
        print(f"เกิดข้อผิดพลาดในการเชื่อมต่อ/เผยแพร่ Google Sheets: {e}")
        print("โปรดตรวจสอบการตั้งค่า API และการแชร์ Spreadsheet ให้ Service Account")


if __name__ == '__main__':
    run_publication_pipeline()