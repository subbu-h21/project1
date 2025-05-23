from flask import Flask, render_template, request, send_file
import pandas as pd
import os
import numpy as np
import time
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from werkzeug.utils import secure_filename

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 20 * 1024 * 1024  # 20 MB

# Shared loader functions

def _load_account(path):
    eng = 'openpyxl' if path.endswith('.xlsx') else 'xlrd'
    df = (pd.read_excel(path, header=None, engine=eng)
            .drop(index=range(14))
            .dropna(axis=1, how='all')
            .reset_index(drop=True))
    df.columns = df.iloc[0]
    df = df.drop(index=0).reset_index(drop=True).drop('Cheque No', axis=1)
    df.columns = ['Date', 'Particular', 'Given', 'Received', 'Balance']
    return df


def _load_books(path):
    eng = 'openpyxl' if path.endswith('.xlsx') else 'xlrd'
    df = (pd.read_excel(path, header=None, engine=eng)
            .dropna(axis=1, how='all')
            .reset_index(drop=True))
    df.columns = df.iloc[0]
    df = df.drop(index=0).reset_index(drop=True)
    # remove opening/closing
    df = df[~df['Particular'].str.contains('Opening balance|Closing balance', case=False, na=False)]
    return df


def save_upload(file):
    filename = secure_filename(file.filename)
    path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(path)
    return path

# Process receivements discrepancies

def process_receivements(ac_path, books_path):
    ac = _load_account(ac_path)
    ob = _load_books(books_path)

    # filter receivements
    recv_ac = ac[ac['Received'].notna()].copy()
    recv_ob = ob[ob['Debit'].ne(0)].copy()

    # clean names
    recv_ac['SupplierName'] = recv_ac['Particular'].str.strip()
    recv_ob['Particular'] = (recv_ob['Particular']
        .str.extract(r"\((.*?)\)?$")[0]
        .fillna(recv_ob['Particular'])
        .str.strip())

    # parse dates
    recv_ac['Date'] = pd.to_datetime(recv_ac['Date'], dayfirst=True, errors='coerce').dt.date
    recv_ob['Date'] = pd.to_datetime(recv_ob['Date'], dayfirst=True, errors='coerce').dt.date

    # clean numeric
    recv_ac['Received'] = (recv_ac['Received']
        .replace({'[$,\s]+': ''}, regex=True)
        .pipe(pd.to_numeric, errors='coerce'))
    recv_ob['Debit'] = (recv_ob['Debit']
        .replace({'[$,\s]+': ''}, regex=True)
        .pipe(pd.to_numeric, errors='coerce'))

    # group sums
    sum_ac = recv_ac.groupby(['Date','SupplierName'], sort=False)['Received'].sum().reset_index()
    sum_ob = recv_ob.groupby(['Date','Particular'], sort=False)['Debit'].sum().reset_index()

    # build base discrepancy table
    dates = sorted(set(recv_ac['Date'].dropna()).union(recv_ob['Date'].dropna()))
    rows = []
    for d in dates:
        a_names = set(recv_ac.loc[recv_ac['Date']==d, 'SupplierName'])
        b_names = set(recv_ob.loc[recv_ob['Date']==d, 'Particular'])
        only_ob = list(b_names - a_names)
        only_ac = list(a_names - b_names)
        L = max(len(only_ob), len(only_ac))
        only_ob += [None] * (L - len(only_ob))
        only_ac += [None] * (L - len(only_ac))
        for obn, acn in zip(only_ob, only_ac):
            rows.append({'Date': d, 'Not in account statement': obn, 'Not in our books': acn})
    df = pd.DataFrame(rows)

    # merge sums via vectorized joins
    df = (df
        .merge(sum_ob, left_on=['Date','Not in account statement'], right_on=['Date','Particular'], how='left')
        .drop(columns=['Particular'])
        .merge(sum_ac, left_on=['Date','Not in our books'], right_on=['Date','SupplierName'], how='left')
        .drop(columns=['SupplierName'])
        .rename(columns={'Debit':'Debit', 'Received':'Received'})
        .sort_values('Date')
        .reset_index(drop=True)
    )

    # insert blank rows where date changes
    df['prev_date'] = df['Date'].shift()
    blanks = df['Date'] != df['prev_date']
    out = []
    for is_blank, row in zip(blanks, df.to_dict('records')):
        if is_blank and out:
            out.append({'Date': None, 'Not in account statement': None, 'Not in our books': None, 'Debit': None, 'Received': None})
        out.append(row)
    final = pd.DataFrame(out).drop(columns=['prev_date'])

    # filter out UPI rows
    mask = ~final['Not in account statement'].str.contains('UPI:', na=False) & ~final['Not in our books'].str.contains('UPI:', na=False)
    final = final.loc[mask].reset_index(drop=True)
    # add blank column for formatting
    final[''] = None
    return final[['Date','Not in account statement','Debit','','Not in our books','Received']]

# Process payments discrepancies

def process_payments(ac_path, books_path):
    ac = _load_account(ac_path)
    ob = _load_books(books_path)

    pay_ac = ac[ac['Given'].notna()].copy()
    pay_ob = ob[ob['Credit'].ne(0)].copy()

    # clean particulars & supplier
    pay_ac['Particular'] = pay_ac['Particular'].str.replace(r"(NEFT-|MClick/To\s+)", "", regex=True).str.strip()
    pay_ac['SupplierName'] = pay_ac['Particular'].str.split('/', n=1).str[0]
    pay_ob['Particular'] = pay_ob['Particular'].str.strip()

    # parse dates
    pay_ac['Date'] = pd.to_datetime(pay_ac['Date'], dayfirst=True, errors='coerce').dt.date
    pay_ob['Date'] = pd.to_datetime(pay_ob['Date'], dayfirst=True, errors='coerce').dt.date

    # clean numeric
    pay_ac['Given'] = (pay_ac['Given']
        .replace({'[$,\s]+': ''}, regex=True)
        .pipe(pd.to_numeric, errors='coerce'))
    pay_ob['Credit'] = (pay_ob['Credit']
        .replace({'[$,\s]+': ''}, regex=True)
        .pipe(pd.to_numeric, errors='coerce'))

    # sums
    sum_ac = pay_ac.groupby(['Date','SupplierName'], sort=False)['Given'].sum().reset_index()
    sum_ob = pay_ob.groupby(['Date','Particular'], sort=False)['Credit'].sum().reset_index()

    # base table
    dates = sorted(set(pay_ac['Date'].dropna()).union(pay_ob['Date'].dropna()))
    rows = []
    for d in dates:
        names_ac = set(pay_ac.loc[pay_ac['Date']==d, 'SupplierName'])
        names_ob = set(pay_ob.loc[pay_ob['Date']==d, 'Particular'])
        only_ob = list(names_ob)
        only_ac = list(names_ac)
        L = max(len(only_ob), len(only_ac))
        only_ob += [None] * (L - len(only_ob))
        only_ac += [None] * (L - len(only_ac))
        for obn, acn in zip(only_ob, only_ac):
            rows.append({'Date': d, 'Not in account statement': obn, 'Not in our books': acn})
    df = pd.DataFrame(rows)

    # vectorized joins
    df = (df
        .merge(sum_ob, left_on=['Date','Not in account statement'], right_on=['Date','Particular'], how='left')
        .drop(columns=['Particular'])
        .merge(sum_ac, left_on=['Date','Not in our books'], right_on=['Date','SupplierName'], how='left')
        .drop(columns=['SupplierName'])
        .rename(columns={'Credit':'Credit', 'Given':'Given'})
        .sort_values('Date')
        .reset_index(drop=True)
    )

    # blanks insertion
    df['prev_date'] = df['Date'].shift()
    blanks = df['Date'] != df['prev_date']
    out = []
    for is_blank, row in zip(blanks, df.to_dict('records')):
        if is_blank and out:
            out.append({'Date':None,'Not in account statement':None,'Not in our books':None,'Credit':None,'Given':None})
        out.append(row)
    final = pd.DataFrame(out).drop(columns=['prev_date'])

    final[''] = None
    return final[['Date','Not in account statement','Credit','','Not in our books','Given']]

# Process summary

def process_summary(ac_path, books_path):
    ac = _load_account(ac_path)
    ob = _load_books(books_path)

    # parse dates
    ac['Date'] = pd.to_datetime(ac['Date'], dayfirst=True, errors='coerce')
    ob['Date'] = pd.to_datetime(ob['Date'], dayfirst=True, errors='coerce')

    # clean numeric
    ac[['Given','Received']] = (ac[['Given','Received']]
        .replace({'[$,\s]+':''}, regex=True)
        .replace(['0',''], np.nan)
        .apply(pd.to_numeric, errors='coerce'))
    ob[['Credit','Debit']] = (ob[['Credit','Debit']]
        .replace({'[$,\s]+':''}, regex=True)
        .replace([0,''], np.nan)
        .apply(pd.to_numeric, errors='coerce'))

    ac['Balance'] = (ac['Balance']
        .str.replace(',', '', regex=True)
        .astype(float)
        .abs())
    ob['Balance'] = (ob['Balance']
        .str.replace('Cr','', regex=False)
        .str.replace(',', '', regex=False)
        .str.replace(r'\s+','', regex=True)
        .astype(float)
        .abs())

    # collect metrics per date
    all_dates = sorted(set(ac['Date'].dt.date.dropna()).union(ob['Date'].dt.date.dropna()))
    records = []
    for d in all_dates:
        ac_d = ac[ac['Date'].dt.date == d]
        ob_d = ob[ob['Date'].dt.date == d]
        metrics = [
            ('total count', len(ac_d), len(ob_d)),
            ('debit/received entries', ac_d['Received'].count(), ob_d['Debit'].count()),
            ('credit/given entries', ac_d['Given'].count(), ob_d['Credit'].count()),
            ('debit/received total', ac_d['Received'].sum(), ob_d['Debit'].sum()),
            ('credit/given total', ac_d['Given'].sum(), ob_d['Credit'].sum()),
            ('Closing Balance', ac_d['Balance'].iloc[0] if not ac_d.empty else np.nan,
                                 ob_d['Balance'].iloc[-1] if not ob_d.empty else np.nan)
        ]
        for name,a,o in metrics:
            records.append((d, name, a, o))

    idx = pd.MultiIndex.from_tuples([(d,m) for d,m,_,_ in records], names=['Date','Metric'])
    df = pd.DataFrame([(a-o) for *_,a,o in records], index=idx, columns=['Difference'])
    df[['account','our_book']] = pd.DataFrame([(a,o) for *_,a,o in records], index=idx)
    return df

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process_files():
    files = request.files
    if 'ac_statement' not in files or 'our_books' not in files:
        return "Error: Both files are required!"
    ac_path = save_upload(files['ac_statement'])
    ob_path = save_upload(files['our_books'])
    timestamp = int(time.time())
    out_path = os.path.join(app.config['UPLOAD_FOLDER'], f"combined_{timestamp}.xlsx")

    recv_df = process_receivements(ac_path, ob_path)
    pay_df = process_payments(ac_path, ob_path)
    summary_df = process_summary(ac_path, ob_path)

    with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
        recv_df.to_excel(writer, sheet_name='Receivements', index=False)
        pay_df.to_excel(writer, sheet_name='Payments', index=False)
        summary_df.to_excel(writer, sheet_name='Summary', index=True, index_label=['Date','Metric'])

    wb = load_workbook(out_path)
    grey = PatternFill(start_color="DDDDDD", end_color="DDDDDD", fill_type="solid")
    for ws in wb.worksheets:
        for row in ws.iter_rows(min_row=2, max_row=ws.max_row, min_col=1, max_col=6):
            if row[0].value is None:
                for cell in row:
                    cell.fill = grey
    wb.save(out_path)
    os.remove(ac_path)
    os.remove(ob_path)
    return send_file(out_path, as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)))
