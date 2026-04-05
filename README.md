# 📊 Proving the Relationship: Gold, Oil, and War News

---

## 📌 Overview

โปรเจกต์นี้มุ่งเน้นการทำ Proof-based Analysis เพื่อพิสูจน์ความสัมพันธ์ระหว่าง:

- 🟡 ราคาทองคำ (Gold - THB)
- 🛢️ ราคาน้ำมันดิบ WTI (Oil - USD)
- 📰 ความเข้มข้นของข่าวสงคราม (War News Intensity)

💡 คำถามหลัก:
สงครามทำให้ทองและน้ำมันเคลื่อนไหวไปในทิศทางเดียวกันจริงหรือไม่?

---

## 🎯 Objectives

- ทดสอบทฤษฎี Safe Haven ของทองคำ
- วิเคราะห์ความไวของราคาน้ำมันต่อข่าวสงคราม
- ตรวจสอบความสัมพันธ์ระหว่าง Gold vs Oil

---

## 📂 Project Structure

INTER-MARKET RELATIONSHIP/
│
├── assets/
├── data/
│   ├── raw/
│   ├── processed/
│   └── LICENSE_DATA.md
│
├── src/
│   ├── crawler/
│   │   ├── gold_scraper.py
│   │   └── oil_scraper.py
│   │
│   └── processing/
│       └── data_cleaner.ipynb
│
├── main.ipynb
├── requirements.txt
├── README.md
└── LICENSE

---

## ⚙️ Installation

git clone https://github.com/your-username/proving-gold-oil-war-relationship.git
cd proving-gold-oil-war-relationship
pip install -r requirements.txt

---

## 🚀 Usage

# 1. Data Collection
python src/crawler/gold_scraper.py
python src/crawler/oil_scraper.py

# 2. Data Cleaning
# เปิดไฟล์ src/processing/data_cleaner.ipynb

# 3. Analysis
# เปิดไฟล์ main.ipynb

---

## 📊 Key Findings

War News Paradox:
- Normal Correlation: -0.45
- War Period: ~0.03

=> ความสัมพันธ์ "หายไป" ในช่วงสงคราม

Oil:
- Positive correlation ชัดเจน
- ข่าวมาก → ราคาขึ้น

Gold:
- ไม่ได้ขึ้นตามข่าวเสมอ
- มีความไม่แน่นอนสูง
- Price-in ล่วงหน้า

Dynamic Correlation:
- ความสัมพันธ์ไม่คงที่
- มี spike ในปี 2026

---

## 📈 Visualizations

### Gold & Oil Dynamics
![Gold & Oil](assets/1Comprehensive%20Dynamics.png)

### Rolling Correlation
![Rolling](assets/2Statistical%20Proof.png)

### War News vs Prices
![Scatter](assets/3Theory%20vs.%20Reality%20by%20Asset.png)

### Noise Effect
![Noise](assets/4Visualizing%20Noise.png)

---

## 🛠️ Tech Stack

- Python
- Pandas / NumPy
- Matplotlib / Seaborn
- Jupyter Notebook

---

## 💡 Insight

War ≠ Guaranteed Gold Rally

- War news = Noise
- Gold & Oil ไม่ได้วิ่งคู่กันเสมอ
- Market behavior เปลี่ยนตลอดเวลา

---

## 🚀 Future Work

- NLP วิเคราะห์ข่าว
- VAR / Granger Causality
- เพิ่ม USD, Crypto

---

## 📄 License

- MIT License
- Data: data/LICENSE_DATA.md
