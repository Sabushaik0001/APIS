# 🚀 Warehouse APIs

This repository contains the backend API services for the **Warehouse Management System**.  
Follow the steps below to set up, configure, and run the project on your local machine.

---

## 🧩 1. Prerequisites

Before you begin, make sure the following are installed on your system:

- **Python 3.8+**
- **pip** (Python package manager)
- **Git**

---

## 🏗️ 2. Clone the Repository

```bash
git clone https://github.com/Sabushaik0001/APIS.git
cd APIS
```
## 🧱 3. Create a Virtual Environment

## 🐧 For Linux / macOS:

```bash
python3 -m venv venv
source venv/bin/activate
````
## 🪟 For Windows:
```bash
python -m venv venv
venv\Scripts\activate
```

##📦 4. Install Dependencies
``` bash
pip install -r requirements.txt
```

##⚙️ 5. Run the Application Locally
```bash
uvicorn main:app  --port 8080 --reload
````
