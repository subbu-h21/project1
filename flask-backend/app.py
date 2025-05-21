import os
import time
import logging
import psutil
from io import BytesIO
from flask import Flask, render_template, request, send_file, abort
import pandas as pd
import re
from openpyxl import load_workbook
from openpyxl.styles import PatternFill

# ——— Logging configuration ———
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(module)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# where we’ll store outputs (no disk uploads)
OUTPUT_FOLDER = 'outputs'
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
app.config['OUTPUT_FOLDER'] = OUTPUT_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 20 * 1024 * 1024  # 20 MB limit

# Shared helper: memory log
def log_memory(stage: str):
    proc = psutil.Process(os.getpid())
    mb = proc.memory_info().rss / 1024 ** 2
    logger.info(f"Memory usage {stage}: {mb:.1f} MB")

# Styling helper
def apply_styling(wb):
    grey = PatternFill(start_color="DDDDDD", end_color="DDDDDD", fill_type="solid")
    for ws in wb.worksheets:
        for i in range(2, ws.max_row + 1):
            if ws.cell(row=i, column=1).value is None:
                for j in range(1, ws.max_column + 1):
                    ws.cell(row=i, column=j).fill = grey

# Streamed read into DataFrame
def load_sheet_to_df(path, skip_rows=0):
    wb = load_workbook(path, read_only=True)
    ws = wb.active
    rows = []
    for idx, row in enumerate(ws.iter_rows(values_only=True)):
        if idx < skip_rows:
            continue
        rows.append(row)
    df = pd.DataFrame(rows)
    wb.close()
    return df

# 1st: receivements discrepancy using streamed loads
def process_receivements(ac_path, books_path):
    # 1) Load and preprocess account statement
    log_memory('before receivements load')
    ac = load_sheet_to_df(ac_path, skip_rows=14)
    ac.dropna(axis=1, how='all', inplace=True)
    ac.columns = ac.iloc[0]
    ac = ac[1:].reset_index(drop=True)
    if 'Cheque No' in ac.columns:
        ac.drop('Cheque No', axis=1, inplace=True)
    ac.columns = ['Date','Particular','Given','Received','Balance']
    
    # 2) Load and preprocess our books
    ob = load_sheet_to_df(books_path, skip_rows=1)
    ob.dropna(axis=1, how='all', inplace=True)
    ob.columns = ob.iloc[0]
    ob = ob[1:].reset_index(drop=True)
    ob = ob[~ob['Particular'].str.contains('Opening|Closing balance', case=False, na=False)]
    
    # 3) Filter and clean
    recv_ac = ac[ac['Received'].notna()].copy()
    recv_ob = ob[ob['Debit'] != 0].copy()
    recv_ob['Particular'] = recv_ob['Particular'].str.strip()
    recv_ac['SupplierName'] = recv_ac['Particular'].str.strip()
    recv_ob['Particular'] = recv_ob['Particular'].str.extract(r"\((.*?)\)?$")[0].fillna(recv_ob['Particular'])

    recv_ac['Date'] = pd.to_datetime(recv_ac['Date'], errors='coerce', dayfirst=True).dt.date
    recv_ob['Date'] = pd.to_datetime(recv_ob['Date'], errors='coerce', dayfirst=True).dt.date
    
    # 4) Build discrepancy list
    discs = []
    for d in sorted(set(recv_ac['Date']).intersection(recv_ob['Date'])):
        a = recv_ac[recv_ac['Date']==d]
        b = recv_ob[recv_ob['Date']==d]
        names_ac = set(a['SupplierName'])
        names_ob = set(b['Particular'])
        only_ob = list(names_ob - names_ac)
        only_ac = list(names_ac - names_ob)
        L = max(len(only_ob), len(only_ac))
        only_ob += [None] * (L - len(only_ob))
        only_ac += [None] * (L - len(only_ac))
        for obn, acn in zip(only_ob, only_ac):
            discs.append({'Date':d,'Not in account statement':obn,'Not in our books':acn})
    df = pd.DataFrame(discs)

    # 5) Sum amounts
    recv_ac['Received'] = pd.to_numeric(recv_ac['Received'].replace({'[$,\s]': ''}, regex=True), errors='coerce')
    recv_ob['Debit'] = pd.to_numeric(recv_ob['Debit'].replace({'[$,\s]': ''}, regex=True), errors='coerce')
    sum_ac = recv_ac.groupby(['Date','SupplierName'])['Received'].sum().reset_index()
    sum_ob = recv_ob.groupby(['Date','Particular'])['Debit'].sum().reset_index()
    df = df.merge(sum_ac, left_on=['Date','Not in our books'], right_on=['Date','SupplierName'], how='left')
    df = df.merge(sum_ob, left_on=['Date','Not in account statement'], right_on=['Date','Particular'], how='left')
    df.drop(columns=['SupplierName','Particular'], inplace=True)
    
    # 6) Final formatting
    rows, prev = [], None
    for _, r in df.sort_values('Date').iterrows():
        if prev and r['Date'] != prev:
            rows.append({'Date':None,'Not in account statement':None,'Not in our books':None,'Credit':None,'Received':None})
        rows.append(r.to_dict())
        prev = r['Date']
    final = pd.DataFrame(rows)
    final[''] = None
    final = final[['Date','Not in account statement','Debit','', 'Not in our books','Received']]
    final = final[~final['Not in account statement'].str.contains('UPI:', na=False) &
                  ~final['Not in our books'].str.contains('UPI:', na=False)]
    log_memory('after receivements')
    return final

# 2nd: payments discrepancy (streamed)
def process_payments(ac_path, books_path, timestamp):
    log_memory('before payments load')
    ac = load_sheet_to_df(ac_path, skip_rows=14)
    ac.dropna(axis=1, how='all', inplace=True)
    ac.columns = ac.iloc[0]; ac = ac[1:].reset_index(drop=True)
    ac.drop(columns=['Cheque No'], errors='ignore', inplace=True)
    ac.columns = ['Date','Particular','Given','Received','Balance']

    ob = load_sheet_to_df(books_path, skip_rows=1)
    ob.dropna(axis=1, how='all', inplace=True)
    ob.columns = ob.iloc[0]; ob = ob[1:].reset_index(drop=True)
    ob = ob[~ob['Particular'].str.contains('Opening|Closing balance', case=False, na=False)]

    pay_ac = ac[ac['Given'].notna()].copy()
    pay_ob = ob[ob['Credit'] != 0].copy()
    pay_ob['Particular'] = pay_ob['Particular'].str.strip()

    pay_ac['Particular'] = pay_ac['Particular'].apply(lambda x: re.sub(r"(NEFT-|MClick/To\s+)", "", str(x)))
    pay_ac['SupplierName'] = pay_ac['Particular'].str.split('/').str[0]
    pay_ac['Date'] = pd.to_datetime(pay_ac['Date'], errors='coerce', dayfirst=True).dt.date
    pay_ob['Date'] = pd.to_datetime(pay_ob['Date'], errors='coerce', dayfirst=True).dt.date

    discs = []
    for d in sorted(set(pay_ac['Date']).intersection(pay_ob['Date'])):
        a = pay_ac[pay_ac['Date']==d]; b = pay_ob[pay_ob['Date']==d]
        n_ac = set(a['SupplierName']); n_ob = set(b['Particular'])
        only_ob = list(n_ob - n_ac); only_ac = list(n_ac - n_ob)
        L = max(len(only_ob), len(only_ac))
        only_ob += [None] * (L - len(only_ob)); only_ac += [None] * (L - len(only_ac))
        for obn, acn in zip(only_ob, only_ac):
            discs.append({'Date':d,'Not in account statement':obn,'Not in our books':acn})
    df = pd.DataFrame(discs)

    pay_ac['Given'] = pd.to_numeric(pay_ac['Given'].replace({'[$,\s]': ''}, regex=True), errors='coerce')
    pay_ob['Credit'] = pd.to_numeric(pay_ob['Credit'].replace({'[$,\s]': ''}, regex=True), errors='coerce')
    sum_ac = pay_ac.groupby(['Date','SupplierName'])['Given'].sum().reset_index()
    sum_ob = pay_ob.groupby(['Date','Particular'])['Credit'].sum().reset_index()
    df = df.merge(sum_ac, left_on=['Date','Not in our books'], right_on=['Date','SupplierName'], how='left')
    df = df.merge(sum_ob, left_on=['Date','Not in account statement'], right_on=['Date','Particular'], how='left')
    df.drop(columns=['SupplierName','Particular'], inplace=True)

    rows, prev = [], None
    for _, r in df.sort_values('Date').iterrows():
        if prev and r['Date'] != prev:
            rows.append({'Date':None,'Not in account statement':None,'Not in our books':None,'Credit':None,'Given':None})
        rows.append(r.to_dict()); prev = r['Date']
    final = pd.DataFrame(rows)
    final[''] = None
    final = final[['Date','Not in account statement','Credit','', 'Not in our books','Given']]
    log_memory('after payments')
    return final

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process_files():
    try:
        start = time.perf_counter()
        ac_file = request.files.get('ac_statement')
        ob_file = request.files.get('our_books')
        if not ac_file or not ob_file:
            abort(400, 'Both files are required')

        # read into memory
        ac_io = BytesIO(ac_file.read())
        ob_io = BytesIO(ob_file.read())

        # process
        recv_df = process_receivements(ac_io, ob_io)
        pay_df = process_payments(ac_io, ob_io, int(time.time()))

        # write output
        timestamp = int(time.time())
        out_path = os.path.join(app.config['OUTPUT_FOLDER'], f'combined_{timestamp}.xlsx')
        with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
            recv_df.to_excel(writer, sheet_name='Receivements', index=False)
            pay_df.to_excel(writer, sheet_name='Payments', index=False)

        # styling only in non-production
        if os.getenv('FLASK_ENV') != 'production':
            wb = load_workbook(out_path)
            apply_styling(wb)
            wb.save(out_path)

        elapsed = time.perf_counter() - start
        logger.info(f"Total /process took {elapsed:.2f}s")
        return send_file(out_path, as_attachment=True)

    except Exception as e:
        logger.exception("Error in /process")
        abort(500, str(e))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)))
