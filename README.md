# GIS Data Ingestion System

מערכת לקליטת נתוני תכניות מ-FGDB לפורטל הגאוגרפי (PostgreSQL/PostGIS)

## תיאור המערכת

המערכת סורקת תיקיות עם קבצי FGDB (File Geodatabase) ומקלטת אותם למסד נתונים PostgreSQL/PostGIS.

### תהליך העבודה:

1. **סריקת תיקיות**: המערכת סורקת את הנתיב `O:\Center\Excavations` ומחפשת תיקיות שמתחילות ב-`A-`

2. **חיפוש משאבי GIS**: בכל תיקייה, המערכת מחפשת:
   - תיקיית `GIS`
   - קבצים דחוסים (`.7z`, `.zip`, `.rar`)
   - תיקיות GDB (`.gdb`)

3. **טיפול בקבצים דחוסים**: אם נמצא קובץ דחוס, המערכת פותחת אותו באופן אוטומטי

4. **קליטת נתונים**: עבור כל GDB:
   - בדיקה אם הטבלה קיימת במסד הנתונים
   - אם לא - יצירת טבלה חדשה עם השדות הבאים:
     - כל השדות המקוריים מה-GDB
     - `source_directory` - נתיב מקור הנתונים (עד תיקיית ה-A-)
     - `ingestion_datetime` - תאריך ושעת קליטה
     - `ingestion_batch_id` - מספר פנימי רץ למופע קליטה
     - `fgdb_name` - שם ה-FGDB
   - אם הטבלה קיימת:
     - בדיקה שהנתונים לא נקלטו כבר
     - בדיקה שהשדות תואמים בדיוק
     - אם השדות לא תואמים - רישום ללוג והמשך לשכבה הבאה

## התקנה

### דרישות מקדימות:

1. **Python 3.8+**
2. **PostgreSQL/PostGIS**
3. **GDAL/OGR** - עבור קריאת קבצי GDB

### התקנת החבילות:

```bash
pip install -r requirements.txt
```

**הערה חשובה**: התקנת GDAL עשויה להיות מסובכת. בסביבת Windows, מומלץ להשתמש ב:
- Conda: `conda install -c conda-forge gdal`
- או להוריד wheel files מ-https://www.lfd.uci.edu/~gohlke/pythonlibs/#gdal

### התקנת כלי עזר לקבצים דחוסים:

עבור קבצי RAR, יש להתקין את WinRAR או UnRAR:
- Windows: הורד מ-https://www.rarlab.com/
- Linux: `sudo apt-install unrar`

## הגדרת קונפיגורציה

ערוך את הקובץ `config.py` ועדכן את הפרטים הבאים:

```python
# PostgreSQL Configuration
POSTGRES_CONFIG = DBConfig(
    host="your_host",
    user="your_username",
    password="your_password",
    db_name="iaagis_prod01",
    port=5432
)

# File paths
ROOT_PATH = r"O:\Center\Excavations"
```

## הרצה

```bash
python main.py
```

## קבצי הלוגים

הלוגים נשמרים בתיקייה `logs/` עם חותמת זמן:
```
logs/gis_ingestion_20250930_143000.log
```

## מבנה הקוד

```
├── main.py                 # נקודת הכניסה הראשית
├── config.py              # הגדרות קונפיגורציה
├── logger_setup.py        # הגדרת מערכת הלוגים
├── database.py            # פעולות מסד נתונים
├── file_scanner.py        # סריקת קבצים ותיקיות
├── gdb_handler.py         # טיפול בקבצי GDB
├── requirements.txt       # תלויות Python
└── README.md             # תיעוד
```

## פתרון בעיות נפוצות

### שגיאת חיבור למסד נתונים

ודא ש:
- PostgreSQL פועל
- הפרטים ב-`config.py` נכונים
- יש לך הרשאות גישה למסד הנתונים

### שגיאה בקריאת GDB

ודא ש:
- GDAL מותקן כראוי
- יש לך גישה לקובץ ה-GDB
- הקובץ תקין ולא פגום

### שגיאה בפתיחת קבצים דחוסים

ודא ש:
- כלי החילוץ מותקנים (WinRAR, 7-Zip)
- יש לך הרשאות כתיבה בתיקייה
- הקובץ הדחוס לא פגום

