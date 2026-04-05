import requests
import pandas as pd
from io import StringIO
import time
import random
import os  # สำหรับจัดการ Path และสร้าง Folder

# --- ตั้งค่าเริ่มต้น ---
# ใช้ชื่อเดือนเต็มตามโครงสร้าง URL ของเว็บไซต์
thai_months = [
    'มกราคม', 'กุมภาพันธ์', 'มีนาคม', 'เมษายน', 'พฤษภาคม', 'มิถุนายน',
    'กรกฎาคม', 'สิงหาคม', 'กันยายน', 'ตุลาคม', 'พฤศจิกายน', 'ธันวาคม'
]

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# กำหนด Folder ปลายทาง
output_dir = '../data/raw/gold_raw'

# สร้าง Folder หากยังไม่มีในเครื่อง
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
    print(f"สร้างโฟลเดอร์ใหม่ที่: {output_dir}")

print("เริ่มภารกิจดูดข้อมูลทองคำแบบแยกไฟล์รายปี...")

# ลูปชั้นนอก: วนตามปี (2564 - 2569)
for year in range(2564, 2570): 

    yearly_raw_data = [] # รีเซ็ตลิสต์เก็บข้อมูลสำหรับปีใหม่
    print(f"\n--- 📅 เริ่มเก็บข้อมูลของปี {year} ---")

    # ลูปชั้นใน: วนตามเดือน
    for month in thai_months:
        # URL ตามรูปแบบของเว็บไซต์
        url = f"https://xn--42cah7d0cxcvbbb9x.com/ราคาทองย้อนหลัง-เดือน-{month}-{year}/"
        print(f"กำลังดูดข้อมูล: {month}...", end=" ", flush=True)

        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.encoding = 'utf-8'

            if response.status_code == 200:
                html_data = StringIO(response.text)
                tables = pd.read_html(html_data)

                if len(tables) > 0:
                    df_raw = tables[0]

                    # แปะป้ายบอกเดือนและปีไว้ในตาราง (เผื่อใช้ตอน Data Cleaning)
                    df_raw['Month_Scraped'] = month
                    df_raw['Year_Scraped'] = year

                    yearly_raw_data.append(df_raw)
                    print(f"✅ สำเร็จ ({len(df_raw)} แถว)")
                else:
                    print("⚠ ไม่พบตารางข้อมูลในหน้านี้")
            else:
                print(f"❌ ไม่พบหน้าเว็บ (Status: {response.status_code})")

        except Exception as e:
            print(f"❌ Error: {e}")

        # สุ่มเวลาพัก 1-3 วินาที เพื่อถนอม Server และป้องกันการโดนแบน IP
        time.sleep(random.uniform(1, 3))

    # บันทึกไฟล์เมื่อดึงครบทุกเดือนในปีนั้นๆ
    if len(yearly_raw_data) > 0:
        # รวม DataFrame ทุกเดือนเป็นก้อนเดียวของปีนั้น
        final_yearly_df = pd.concat(yearly_raw_data, ignore_index=True)
        
        # กำหนดชื่อไฟล์และเส้นทางบันทึก
        filename = f'gold_raw_uncleaned_{year}.csv'
        file_path = os.path.join(output_dir, filename)

        # บันทึกเป็น CSV (ใช้ utf-8-sig เพื่อให้เปิดใน Excel ภาษาไทยได้ไม่เป็นภาษาต่างดาว)
        final_yearly_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"\n💾 [สำเร็จ] บันทึกไฟล์ปี {year} ไปที่: {file_path}")
        print(f"📊 รวมจำนวนข้อมูลทั้งหมดในปีนี้: {len(final_yearly_df)} แถว")
    else:
        print(f"\n⚠ ปี {year} ไม่มีข้อมูลให้บันทึกเลย")

print("\n" + "="*50)
print("🎉 เสร็จสิ้น! ข้อมูลทั้งหมดถูกเก็บไว้ใน ../data/gold_raw/ เรียบร้อยแล้ว")