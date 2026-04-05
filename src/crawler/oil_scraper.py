import yfinance as yf
import pandas as pd
import os
from datetime import datetime

# 1. กำหนดโฟลเดอร์ปลายทาง
output_dir = '../data/raw/oil_raw'

# สร้างโฟลเดอร์ถ้ายังไม่มี (รวมถึงโฟลเดอร์แม่ด้วย)
os.makedirs(output_dir, exist_ok=True)

# 2. กำหนดช่วงปี พ.ศ. 2564 - 2569
years = range(2564, 2570) 

print(f"🛢️ เริ่มภารกิจดึงข้อมูลน้ำมันดิบโลก (WTI) ไปยัง: {output_dir}")

for year_th in years:
    year_en = year_th - 543  # แปลง พ.ศ. เป็น ค.ศ.
    start_date = f"{year_en}-01-01"
    
    # กำหนดวันสิ้นสุด
    # ถ้าเป็นปีปัจจุบัน (2026) ให้ดึงถึงแค่วันนี้
    current_year_en = datetime.now().year
    if year_en == current_year_en:
        end_date = datetime.now().strftime('%Y-%m-%d')
    else:
        end_date = f"{year_en}-12-31"

    print(f"\n📅 กำลังดึงข้อมูลปี {year_th} (ค.ศ. {year_en})...")
    print(f"   ช่วงเวลา: {start_date} ถึง {end_date}")

    try:
        # ดึงข้อมูลจาก Yahoo Finance (Ticker: CL=F คือ WTI Crude Oil)
        # auto_adjust=True เพื่อให้ได้ราคาที่ปรับแต่งแล้ว (Close)
        oil_df = yf.download('CL=F', start=start_date, end=end_date, progress=False)

        if not oil_df.empty:
            # รีเซ็ต Index เพื่อให้ Date กลายเป็นคอลัมน์ปกติ
            oil_df = oil_df.reset_index()

            # แปะป้ายบอกปี พ.ศ. (แบบดิบๆ ให้น้องไปคลีนต่อ)
            oil_df['Year_Scraped'] = year_th

            # กำหนดชื่อไฟล์และ Path บันทึก
            filename = f'oil_wti_raw_{year_th}.csv'
            file_path = os.path.join(output_dir, filename)

            # บันทึกเป็น CSV (ใช้ utf-8-sig เพื่อให้เปิดใน Excel ได้ทันที)
            oil_df.to_csv(file_path, index=False, encoding='utf-8-sig')
            
            print(f"   ✅ บันทึกสำเร็จ: {file_path}")
            print(f"   📊 จำนวนข้อมูล: {len(oil_df)} แถว")
        else:
            print(f"   ❌ ไม่พบข้อมูลสำหรับปี {year_th}")

    except Exception as e:
        print(f"   ❌ เกิดข้อผิดพลาด: {e}")

print("\n" + "="*50)
print("🎉 เสร็จสิ้น! ได้ไฟล์น้ำมันโลกครบถ้วนแล้ว")