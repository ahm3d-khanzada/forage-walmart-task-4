# 🚚 Walmart Shipment Data Processing

This project processes shipping data from Walmart using **pandas** and stores it in a **SQLite** database. It combines data from multiple CSV files and inserts structured records into a single database table.

---

## 📁 Project Structure

```bash
forage-walmart-task-4/
│
├── data/
│ ├── shipping_data_0.csv
│ ├── shipping_data_1.csv
│ └── shipping_data_2.csv
│
├── shipment_database.db # SQLite database (generated)
├── process_shipments.py # Python script to run
└── README.md
```


---

## ⚙️ Technologies Used

- 🐍 Python 3.x
- 📦 pandas
- 🗄️ SQLite (via `sqlite3`)

---

## 📦 How It Works

1. **Reads** 3 CSV files:
   - `shipping_data_0.csv` – Main shipment records
   - `shipping_data_1.csv` – Contains origin, destination
   - `shipping_data_2.csv` – Contains product and driver info

2. **Merges** `shipping_data_1.csv` and `shipping_data_2.csv` on `shipment_identifier`.

3. **Calculates** the quantity of products per shipment.

4. **Creates** a table `shipments` in the SQLite database (if it doesn't already exist).

5. **Inserts**:
   - Data from `shipping_data_0.csv` with product quantities.
   - Combined data from the merged result of CSV1 and CSV2.

---

## ▶️ How to Run

### 🔹 1. Clone the Repo

```bash
git clone https://github.com/yourusername/forage-walmart-task-4.git
cd forage-walmart-task-4
