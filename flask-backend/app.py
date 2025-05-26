from flask import Flask, render_template, request, send_file
import pandas as pd
import re
import os
import numpy as np
import time
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from werkzeug.utils import secure_filename

app = Flask(__name__)

# where we’ll store uploads and outputs
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 20 * 1024 * 1024  # 20 MB limit

# Shared helper: save uploaded files
def save_upload(file):
    path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file.filename))
    file.save(path)
    return path

# 1st: receivements discrepancy
def process_receivements(ac_path, books_path, timestamp):
    # engines
    ac_eng = 'openpyxl' if ac_path.endswith('.xlsx') else 'xlrd'
    ob_eng = 'openpyxl' if books_path.endswith('.xlsx') else 'xlrd'
    # load account statement
    ac = (pd.read_excel(ac_path, header=None, engine=ac_eng)
            .drop(index=range(14)).dropna(axis=1, how='all').reset_index(drop=True))
    ac.columns = ac.iloc[0]; ac = ac[1:].reset_index(drop=True).drop('Cheque No', axis=1)
    ac.columns = ['Date','Particular','Given','Received','Balance']
    # load our books
    ob = (pd.read_excel(books_path, header=None, engine=ob_eng)
            .dropna(axis=1, how='all')
            .pipe(lambda df: df.rename(columns=df.iloc[0])).drop(index=0)
            .pipe(lambda df: df[~df['Particular'].str.contains('Opening balance|Closing balance', case=False, na=False)])
            .reset_index(drop=True))
    # filter receivements
    recv_ac = ac[ac['Received'].notna()].copy()
    recv_ob = ob[ob['Debit'] != 0].copy(); recv_ob['Particular'] = recv_ob['Particular'].str.strip()
    # helpers
    def trim(s): return s.strip()
    def inside(text): m = re.search(r'\((.*?)\)?$', str(text)); return m.group(1) if m else str(text)
    recv_ac['SupplierName'] = recv_ac['Particular'].apply(trim)
    recv_ob['Particular']    = recv_ob['Particular'].apply(inside)
    # parse dates
    recv_ac['Date'] = pd.to_datetime(recv_ac['Date'], errors='coerce', dayfirst=True).dt.date
    recv_ob['Date'] = pd.to_datetime(recv_ob['Date'], errors='coerce', dayfirst=True).dt.date
    # discrepancies list
    discs = []
    for d in sorted(set(recv_ac['Date'].dropna()) | set(recv_ob['Date'].dropna())):
        a = recv_ac[recv_ac['Date']==d]; b = recv_ob[recv_ob['Date']==d]
        names_ac = set(a['SupplierName']); names_ob = set(b['Particular'])
        only_ob = list(names_ob - names_ac); only_ac = list(names_ac - names_ob)
        L = max(len(only_ob), len(only_ac))
        only_ob += [None]*(L-len(only_ob)); only_ac += [None]*(L-len(only_ac))
        for obn, acn in zip(only_ob, only_ac): discs.append({'Date':d,'Not in account statement':obn,'Not in our books':acn})
    df = pd.DataFrame(discs).sort_values('Date').reset_index(drop=True)
    # sums
    recv_ac['Received'] = recv_ac['Received'].replace({'\\$':'','\\,':'','\\s+':''}, regex=True).pipe(pd.to_numeric, errors='coerce')
    recv_ob['Debit']    = recv_ob['Debit'].replace({'\\$':'','\\,':'','\\s+':''}, regex=True).pipe(pd.to_numeric, errors='coerce')
    sum_ac = recv_ac.groupby(['Date','SupplierName'])['Received'].sum().reset_index()
    sum_ob = recv_ob.groupby(['Date','Particular'])['Debit'].sum().reset_index()
    df = (df.merge(sum_ac, left_on=['Date','Not in our books'], right_on=['Date','SupplierName'], how='left').drop(columns=['SupplierName'])
            .merge(sum_ob, left_on=['Date','Not in account statement'], right_on=['Date','Particular'], how='left').drop(columns=['Particular'])
            .sort_values('Date').reset_index(drop=True))
    # blank rows and filter UPI
    rows, prev = [], None
    for _,r in df.iterrows():
        if prev and r['Date']!=prev: rows.append({'Date':None,'Not in account statement':None,'Not in our books':None})
        rows.append(r.to_dict()); prev=r['Date']
    final = pd.DataFrame(rows); final[''] = None
    final = final[['Date','Not in account statement','Debit','', 'Not in our books','Received']]
    final = final[~final['Not in account statement'].str.contains('UPI:', na=False) & ~final['Not in our books'].str.contains('UPI:', na=False)]
    return final

# 2nd: payments discrepancy
def process_payments(ac_path, books_path, timestamp):
    ac_eng = 'openpyxl' if ac_path.endswith('.xlsx') else 'xlrd'
    ob_eng = 'openpyxl' if books_path.endswith('.xlsx') else 'xlrd'
    ac = (pd.read_excel(ac_path, header=None, engine=ac_eng).drop(index=range(14)).dropna(axis=1, how='all').reset_index(drop=True))
    ac.columns = ac.iloc[0]; ac = ac[1:].reset_index(drop=True).drop('Cheque No', axis=1)
    ac.columns = ['Date','Particular','Given','Received','Balance']
    ob = (pd.read_excel(books_path, header=None, engine=ob_eng).dropna(axis=1, how='all')
            .pipe(lambda df: df.rename(columns=df.iloc[0])).drop(index=0)
            .pipe(lambda df: df[~df['Particular'].str.contains('Opening balance|Closing balance', case=False, na=False)]).reset_index(drop=True))
    pay_ac = ac[ac['Given'].notna()].copy(); pay_ob = ob[ob['Credit'] != 0].copy()
    pay_ob['Particular'] = pay_ob['Particular'].str.strip()
    def name_clean(desc): return re.sub(r"(NEFT-|MClick/To\s+)", "", str(desc)).strip()
    def sup_name(txn): parts=str(txn).split('/'); return parts[0].strip() if len(parts)>1 else str(txn)
    pay_ac['Particular']=pay_ac['Particular'].apply(name_clean); pay_ac['SupplierName']=pay_ac['Particular'].apply(sup_name)
    pay_ac['Date']=pd.to_datetime(pay_ac['Date'],errors='coerce',dayfirst=True).dt.date
    pay_ob['Date']=pd.to_datetime(pay_ob['Date'],errors='coerce',dayfirst=True).dt.date
    discs=[]
    for d in sorted(set(pay_ac['Date'].dropna()) | set(pay_ob['Date'].dropna())):
        a=pay_ac[pay_ac['Date']==d]; b=pay_ob[pay_ob['Date']==d]
        n_ac=set(a['SupplierName']); n_ob=set(b['Particular'])
        only_ob=list(n_ob); only_ac=list(n_ac)
        L=max(len(only_ob),len(only_ac)); only_ob+=[None]*(L-len(only_ob)); only_ac+=[None]*(L-len(only_ac))
        for obn,acn in zip(only_ob,only_ac): discs.append({'Date':d,'Not in account statement':obn,'Not in our books':acn})
    df=pd.DataFrame(discs).sort_values('Date').reset_index(drop=True)
    pay_ac['Given']=pay_ac['Given'].replace({'\\$':'','\\,':'','\\s+':''}, regex=True).pipe(pd.to_numeric, errors='coerce')
    pay_ob['Credit']=pay_ob['Credit'].replace({'\\$':'','\\,':'','\\s+':''}, regex=True).pipe(pd.to_numeric, errors='coerce')
    sum_ac=pay_ac.groupby(['Date','SupplierName'])['Given'].sum().reset_index(); sum_ob=pay_ob.groupby(['Date','Particular'])['Credit'].sum().reset_index()
    df=(df.merge(sum_ac,left_on=['Date','Not in our books'],right_on=['Date','SupplierName'],how='left').drop(columns=['SupplierName'])
         .merge(sum_ob,left_on=['Date','Not in account statement'],right_on=['Date','Particular'],how='left').drop(columns=['Particular'])
         .sort_values('Date').reset_index(drop=True))
    rows,prev=[],None
    for _,r in df.iterrows():
        if prev and r['Date']!=prev: rows.append({'Date':None,'Not in account statement':None,'Not in our books':None})
        rows.append(r.to_dict()); prev=r['Date']
    final=pd.DataFrame(rows); final['']=None
    final=final[['Date','Not in account statement','Credit','','Not in our books','Given']]
    return final

def process_summary(ac_path, books_path):
    # Determine engines
    ac_eng = 'openpyxl' if ac_path.endswith('.xlsx') else 'xlrd'
    ob_eng = 'openpyxl' if books_path.endswith('.xlsx') else 'xlrd'

    # Load and normalize account statement
    ac = (pd.read_excel(ac_path, header=None, engine=ac_eng)
            .drop(index=range(14))
            .dropna(axis=1, how='all')
            .reset_index(drop=True))
    ac.columns = ac.iloc[0]
    ac = ac[1:].reset_index(drop=True).drop('Cheque No', axis=1)
    ac.columns = ['Date','Particular','Given','Received','Balance']
    ac['Date'] = pd.to_datetime(ac['Date'], errors='coerce', dayfirst=True)

    # Load and normalize our books
    ob = (pd.read_excel(books_path, header=None, engine=ob_eng)
            .dropna(axis=1, how='all')
            .pipe(lambda df: df.rename(columns=df.iloc[0])).drop(index=0)
            .pipe(lambda df: df[~df['Particular'].str.contains('Opening balance|Closing balance', case=False, na=False)])
            .reset_index(drop=True))
    ob['Date'] = pd.to_datetime(ob['Date'], errors='coerce', dayfirst=True)

    # Clean numeric columns
    ac[['Given','Received']] = (ac[['Given','Received']]
        .replace({'\\$':'','\\,':'','\\s+':''}, regex=True)
        .replace([None,'0'], np.nan)
        .apply(pd.to_numeric, errors='coerce'))
    ob[['Credit','Debit']] = (ob[['Credit','Debit']]
        .replace({'\\$':'','\\,':'','\\s+':''}, regex=True)
        .replace([None,0], np.nan)
        .apply(pd.to_numeric, errors='coerce'))

    ac['Balance'] = (
        ac['Balance']
        .str.replace(',', '', regex=True)
        .astype(float)
        .abs()
    )
    ob['Balance'] = (
        ob['Balance']
        .str.replace('Cr', '', regex=False)
        .str.replace(',', '', regex=False)
        .str.replace(r'\s+', '', regex=True)
    )
    ob['Balance'] = pd.to_numeric(ob['Balance'], errors='coerce').abs()
    
    # Gather all unique dates
    all_dates = sorted(set(ac['Date'].dropna().dt.date.unique()) |
                       set(ob['Date'].dropna().dt.date.unique()))

    rows = []
    for d in all_dates:
        ac_d = ac[ac['Date'].dt.date == d]
        ob_d = ob[ob['Date'].dt.date == d]

        rows.append(((d, 'total count'), {
            'account': len(ac_d),
            'our_book': len(ob_d),
        }))
        rows.append(((d, 'debit/received entries'), {
            'account': ac_d['Received'].count(),
            'our_book': ob_d['Debit'].count(),
        }))
        rows.append(((d, 'credit/given entries'), {
            'account': ac_d['Given'].count(),
            'our_book': ob_d['Credit'].count(),
        }))
        rows.append(((d, 'debit/received total'), {
            'account': ac_d['Received'].sum(),
            'our_book': ob_d['Debit'].sum(),
        }))
        rows.append(((d, 'credit/given total'), {
            'account': ac_d['Given'].sum(),
            'our_book': ob_d['Credit'].sum(),
        }))
        rows.append(((d, 'Closing Balance'), {
            'account': ac_d['Balance'].iloc[0] if not ac_d.empty else None,
            'our_book': ob_d['Balance'].iloc[-1] if not ob_d.empty else None,
        }))

    # Build summary DataFrame
    idx = pd.MultiIndex.from_tuples([r[0] for r in rows], names=['Date','Metric'])
    df = pd.DataFrame([r[1] for r in rows], index=idx)
    df['Difference'] = df['account'] - df['our_book']
    # df = df.reset_index()
    return df

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process_files():
    if 'ac_statement' not in request.files or 'our_books' not in request.files:
        return "Error: Both files are required!"
    ac_file, ob_file = request.files['ac_statement'], request.files['our_books']
    ac_path, ob_path = save_upload(ac_file), save_upload(ob_file)
    timestamp = int(time.time())
    out = os.path.join(app.config['UPLOAD_FOLDER'], f"combined_{timestamp}.xlsx")
    recv_df = process_receivements(ac_path, ob_path, timestamp)
    pay_df  = process_payments(ac_path, ob_path, timestamp)
    summary_df = process_summary(ac_path, ob_path)
    with pd.ExcelWriter(out, engine='openpyxl') as w:
        recv_df.to_excel(w, sheet_name='Receivements', index=False)
        pay_df.to_excel(w, sheet_name='Payments', index=False)
        summary_df.to_excel(w, sheet_name='Summary', index=False)
        summary_df.to_excel(w,sheet_name='Summary',index=True,index_label=['Date', 'Metric'])
    # optional styling across both sheets
    wb = load_workbook(out)
    grey = PatternFill(start_color="DDDDDD", end_color="DDDDDD", fill_type="solid")
    for ws in wb.worksheets:
        for i in range(2, ws.max_row+1):
            if ws[f"A{i}"].value is None:
                for col in ws.iter_cols(min_col=1, max_col=6, min_row=i, max_row=i):
                    col[0].fill = grey
    wb.save(out)
    os.remove(ac_path); os.remove(ob_path)
    return send_file(out, as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT',5000)))