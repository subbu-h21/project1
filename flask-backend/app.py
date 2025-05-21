from flask import Flask, render_template, request, send_file
import pandas as pd
import re
import os
import time
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from werkzeug.utils import secure_filename

app = Flask(__name__)

# where we’ll store uploads and outputs
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def process_account_statement(ac_statement_file, our_books_file):
    """
    - Reads the two uploaded Excel files
    - Runs your full datewise discrepancy logic from the very first script
    - Writes styled Excel to disk and returns its filepath
    """
    timestamp = int(time.time())
    output_filename = f"discrepancies_{timestamp}.xlsx"
    output_path = os.path.join(app.config['UPLOAD_FOLDER'], output_filename)

    # Save incoming files to disk so openpyxl can read them
    ac_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(ac_statement_file.filename))
    books_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(our_books_file.filename))
    ac_statement_file.save(ac_path)
    our_books_file.save(books_path)

    # 1. Determine engines
    ac_engine = 'openpyxl' if ac_path.endswith('.xlsx') else 'xlrd'
    books_engine = 'openpyxl' if books_path.endswith('.xlsx') else 'xlrd'

    # 2. Load and clean Account Statement
    ac = (
        pd.read_excel(ac_path, header=None, engine=ac_engine)
          .drop(index=range(14))
          .dropna(axis=1, how='all')
          .reset_index(drop=True)
    )
    ac.columns = ac.iloc[0]
    ac = ac[1:].reset_index(drop=True)
    ac = ac.drop('Cheque No', axis=1)
    ac.columns = ['Date','Particular','Given','Received','Balance']

    # 3. Load and clean Our Books
    ob = (
        pd.read_excel(books_path, header=None, engine=books_engine)
          .dropna(axis=1, how='all')
          .pipe(lambda df: df.rename(columns=df.iloc[0]))
          .drop(index=0)
          .pipe(lambda df: df[~df['Particular'].str.contains('Opening balance|Closing balance', case=False, na=False)])
          .reset_index(drop=True)
    )

    # 4. Extract payments
    pay_ac = ac[ac['Given'].notna()].copy()
    pay_ob = ob[ob['Credit'] != 0].copy()
    pay_ob['Particular'] = pay_ob['Particular'].astype(str).str.strip()

    # 5. Helper functions
    def extract_name(desc):
        return re.sub(r"(NEFT-|MClick/To\s+)", "", str(desc)).strip()

    def extract_supplier_name(txn):
        parts = str(txn).split('/')
        return parts[0].strip() if len(parts)>=2 else str(txn).strip()

    pay_ac['Particular'] = pay_ac['Particular'].apply(extract_name)
    pay_ac['SupplierName'] = pay_ac['Particular'].apply(extract_supplier_name)

    # 6. Parse dates
    pay_ac['Date'] = pd.to_datetime(pay_ac['Date'], errors='coerce', dayfirst=True).dt.date
    pay_ob['Date'] = pd.to_datetime(pay_ob['Date'], errors='coerce', dayfirst=True).dt.date

    # 7. Build datewise discrepancy list
    all_discs = []
    dates = sorted(
        set(pay_ac['Date'].dropna().unique()) &
        set(pay_ob['Date'].dropna().unique())
    )

    for d in dates:
        a = pay_ac[pay_ac['Date']==d]
        b = pay_ob[pay_ob['Date']==d]
        names_ac   = set(a['SupplierName'].unique())
        names_ob   = set(b['Particular'].unique())
        only_ob    = list(names_ob - names_ac)
        only_ac    = list(names_ac - names_ob)
        L = max(len(only_ob), len(only_ac))
        only_ob += [None]*(L-len(only_ob))
        only_ac += [None]*(L-len(only_ac))
        for ob_name, ac_name in zip(only_ob, only_ac):
            all_discs.append({
                'Date': d,
                'Not in account statement': ob_name,
                'Not in our books': ac_name
            })

    df = pd.DataFrame(all_discs).sort_values('Date').reset_index(drop=True)

    # 8. Sum amounts
    pay_ac['Given'] = (
        pay_ac['Given']
          .replace({'\\$':'','\,':'','\\s+':''}, regex=True)
          .pipe(pd.to_numeric, errors='coerce')
    )
    pay_ob['Credit'] = (
        pay_ob['Credit']
          .replace({'\\$':'','\,':'','\\s+':''}, regex=True)
          .pipe(pd.to_numeric, errors='coerce')
    )

    sum_ac = pay_ac.groupby(['Date','SupplierName'])['Given'].sum().reset_index()
    sum_ob = pay_ob.groupby(['Date','Particular'])['Credit'].sum().reset_index()

    df = (
        df
          .merge(sum_ac, left_on=['Date','Not in our books'],
                        right_on=['Date','SupplierName'], how='left')
          .drop(columns=['SupplierName'])
          .merge(sum_ob, left_on=['Date','Not in account statement'],
                        right_on=['Date','Particular'], how='left')
          .drop(columns=['Particular'])
          .sort_values('Date')
          .reset_index(drop=True)
    )

    # 9. Insert blank rows between date groups
    rows, prev = [], None
    for _, r in df.iterrows():
        if prev is not None and r['Date'] != prev:
            rows.append({'Date':None,'Not in account statement':None,
                         'Not in our books':None})
        rows.append(r.to_dict())
        prev = r['Date']
    final = pd.DataFrame(rows)
    final[''] = None
    final = final[['Date','Not in account statement','Credit','',
                   'Not in our books','Given']]

    # 10. Write to Excel
    final.to_excel(output_path, index=False)
    wb = load_workbook(output_path)
    ws = wb.active
    grey = PatternFill(start_color="DDDDDD", end_color="DDDDDD", fill_type="solid")
    for i in range(2, ws.max_row+1):
        if ws[f"A{i}"].value is None:
            for col in "ABCDEF":
                ws[f"{col}{i}"].fill = grey
    wb.save(output_path)

    return output_path

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process_files():
    if 'ac_statement' not in request.files or 'our_books' not in request.files:
        return "Error: Both files are required!"

    ac_file  = request.files['ac_statement']
    ob_file  = request.files['our_books']
    out_path = process_account_statement(ac_file, ob_file)

    # send the styled Excel back
    return send_file(out_path, as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv("PORT",5000)))
