from flask import Flask, render_template, request, send_file
import pandas as pd
import re
import os
import time
import tempfile
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from werkzeug.utils import secure_filename

app = Flask(__name__)

# where we'll store uploads and outputs
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
    try:
        # engines
        ac_eng = 'openpyxl' if ac_path.endswith('.xlsx') else 'xlrd'
        ob_eng = 'openpyxl' if books_path.endswith('.xlsx') else 'xlrd'
        
        # load account statement with optimized settings
        ac = pd.read_excel(ac_path, header=None, engine=ac_eng)
        ac = ac.drop(index=range(14)).dropna(axis=1, how='all').reset_index(drop=True)
        ac.columns = ac.iloc[0]
        ac = ac[1:].reset_index(drop=True)
        
        # Drop the specific column if it exists
        if 'Cheque No' in ac.columns:
            ac = ac.drop('Cheque No', axis=1)
            
        ac.columns = ['Date','Particular','Given','Received','Balance']
        
        # load our books with optimized settings
        ob = pd.read_excel(books_path, header=None, engine=ob_eng)
        ob = ob.dropna(axis=1, how='all')
        ob = ob.rename(columns=ob.iloc[0]).drop(index=0)
        ob = ob[~ob['Particular'].str.contains('Opening balance|Closing balance', case=False, na=False)]
        ob = ob.reset_index(drop=True)
        
        # filter receivements
        recv_ac = ac[ac['Received'].notna()].copy()
        recv_ob = ob[ob['Debit'] != 0].copy()
        recv_ob['Particular'] = recv_ob['Particular'].str.strip()
        
        # helpers
        def trim(s): 
            return s.strip() if isinstance(s, str) else s
            
        def inside(text): 
            if not isinstance(text, str):
                return str(text)
            m = re.search(r'\((.*?)\)?$', text)
            return m.group(1) if m else text
            
        recv_ac['SupplierName'] = recv_ac['Particular'].apply(trim)
        recv_ob['Particular'] = recv_ob['Particular'].apply(inside)
        
        # parse dates - handle errors gracefully
        recv_ac['Date'] = pd.to_datetime(recv_ac['Date'], errors='coerce', dayfirst=True).dt.date
        recv_ob['Date'] = pd.to_datetime(recv_ob['Date'], errors='coerce', dayfirst=True).dt.date
        
        # discrepancies list - optimize with sets
        discs = []
        ac_dates = set(recv_ac['Date'].dropna())
        ob_dates = set(recv_ob['Date'].dropna())
        common_dates = ac_dates.intersection(ob_dates)
        
        for d in sorted(common_dates):
            a = recv_ac[recv_ac['Date']==d]
            b = recv_ob[recv_ob['Date']==d]
            names_ac = set(a['SupplierName'])
            names_ob = set(b['Particular'])
            only_ob = list(names_ob - names_ac)
            only_ac = list(names_ac - names_ob)
            L = max(len(only_ob), len(only_ac))
            only_ob += [None]*(L-len(only_ob))
            only_ac += [None]*(L-len(only_ac))
            
            for obn, acn in zip(only_ob, only_ac):
                discs.append({'Date':d,'Not in account statement':obn,'Not in our books':acn})
                
        df = pd.DataFrame(discs).sort_values('Date').reset_index(drop=True)
        
        # sums - handle numeric conversion safely
        recv_ac['Received'] = recv_ac['Received'].astype(str).replace({'\\$':'','\\,':'','\\s+':''}, regex=True).pipe(pd.to_numeric, errors='coerce')
        recv_ob['Debit'] = recv_ob['Debit'].astype(str).replace({'\\$':'','\\,':'','\\s+':''}, regex=True).pipe(pd.to_numeric, errors='coerce')
        
        sum_ac = recv_ac.groupby(['Date','SupplierName'])['Received'].sum().reset_index()
        sum_ob = recv_ob.groupby(['Date','Particular'])['Debit'].sum().reset_index()
        
        # Optimize the merge operations
        df = df.merge(sum_ac, left_on=['Date','Not in our books'], right_on=['Date','SupplierName'], how='left')
        df = df.drop(columns=['SupplierName'])
        df = df.merge(sum_ob, left_on=['Date','Not in account statement'], right_on=['Date','Particular'], how='left')
        df = df.drop(columns=['Particular'])
        df = df.sort_values('Date').reset_index(drop=True)
        
        # blank rows and filter UPI
        rows, prev = [], None
        for _, r in df.iterrows():
            if prev and r['Date']!=prev:
                rows.append({'Date':None,'Not in account statement':None,'Not in our books':None})
            rows.append(r.to_dict())
            prev = r['Date']
            
        final = pd.DataFrame(rows)
        final[''] = None
        final = final[['Date','Not in account statement','Debit','', 'Not in our books','Received']]
        
        # Safe handling of string contains operation
        mask1 = final['Not in account statement'].isna() | (~final['Not in account statement'].astype(str).str.contains('UPI:', na=False))
        mask2 = final['Not in our books'].isna() | (~final['Not in our books'].astype(str).str.contains('UPI:', na=False))
        final = final[mask1 & mask2]
        
        return final
        
    except Exception as e:
        print(f"Error in process_receivements: {str(e)}")
        # Return empty DataFrame with correct columns if processing fails
        return pd.DataFrame(columns=['Date','Not in account statement','Debit','', 'Not in our books','Received'])

# 2nd: payments discrepancy
def process_payments(ac_path, books_path, timestamp):
    try:
        ac_eng = 'openpyxl' if ac_path.endswith('.xlsx') else 'xlrd'
        ob_eng = 'openpyxl' if books_path.endswith('.xlsx') else 'xlrd'
        
        # Load with optimized settings
        ac = pd.read_excel(ac_path, header=None, engine=ac_eng)
        ac = ac.drop(index=range(14)).dropna(axis=1, how='all').reset_index(drop=True)
        ac.columns = ac.iloc[0]
        ac = ac[1:].reset_index(drop=True)
        
        # Drop the specific column if it exists
        if 'Cheque No' in ac.columns:
            ac = ac.drop('Cheque No', axis=1)
            
        ac.columns = ['Date','Particular','Given','Received','Balance']
        
        ob = pd.read_excel(books_path, header=None, engine=ob_eng)
        ob = ob.dropna(axis=1, how='all')
        ob = ob.rename(columns=ob.iloc[0]).drop(index=0)
        ob = ob[~ob['Particular'].str.contains('Opening balance|Closing balance', case=False, na=False)]
        ob = ob.reset_index(drop=True)
        
        pay_ac = ac[ac['Given'].notna()].copy()
        pay_ob = ob[ob['Credit'] != 0].copy()
        pay_ob['Particular'] = pay_ob['Particular'].str.strip()
        
        def name_clean(desc):
            if not isinstance(desc, str):
                return str(desc)
            return re.sub(r"(NEFT-|MClick/To\s+)", "", desc).strip()
            
        def sup_name(txn):
            if not isinstance(txn, str):
                return str(txn)
            parts = txn.split('/')
            return parts[0].strip() if len(parts) > 1 else txn
            
        pay_ac['Particular'] = pay_ac['Particular'].apply(name_clean)
        pay_ac['SupplierName'] = pay_ac['Particular'].apply(sup_name)
        
        pay_ac['Date'] = pd.to_datetime(pay_ac['Date'], errors='coerce', dayfirst=True).dt.date
        pay_ob['Date'] = pd.to_datetime(pay_ob['Date'], errors='coerce', dayfirst=True).dt.date
        
        # Optimize with sets
        discs = []
        ac_dates = set(pay_ac['Date'].dropna())
        ob_dates = set(pay_ob['Date'].dropna()) 
        common_dates = ac_dates.intersection(ob_dates)
        
        for d in sorted(common_dates):
            a = pay_ac[pay_ac['Date']==d]
            b = pay_ob[pay_ob['Date']==d]
            n_ac = set(a['SupplierName'])
            n_ob = set(b['Particular'])
            only_ob = list(n_ob - n_ac)
            only_ac = list(n_ac - n_ob)
            L = max(len(only_ob), len(only_ac))
            only_ob += [None]*(L-len(only_ob))
            only_ac += [None]*(L-len(only_ac))
            
            for obn, acn in zip(only_ob, only_ac):
                discs.append({'Date':d,'Not in account statement':obn,'Not in our books':acn})
                
        df = pd.DataFrame(discs).sort_values('Date').reset_index(drop=True)
        
        # Safe numeric conversion
        pay_ac['Given'] = pay_ac['Given'].astype(str).replace({'\\$':'','\\,':'','\\s+':''}, regex=True).pipe(pd.to_numeric, errors='coerce')
        pay_ob['Credit'] = pay_ob['Credit'].astype(str).replace({'\\$':'','\\,':'','\\s+':''}, regex=True).pipe(pd.to_numeric, errors='coerce')
        
        sum_ac = pay_ac.groupby(['Date','SupplierName'])['Given'].sum().reset_index()
        sum_ob = pay_ob.groupby(['Date','Particular'])['Credit'].sum().reset_index()
        
        # Optimize merge operations
        df = df.merge(sum_ac, left_on=['Date','Not in our books'], right_on=['Date','SupplierName'], how='left')
        df = df.drop(columns=['SupplierName'])
        df = df.merge(sum_ob, left_on=['Date','Not in account statement'], right_on=['Date','Particular'], how='left')
        df = df.drop(columns=['Particular'])
        df = df.sort_values('Date').reset_index(drop=True)
        
        rows, prev = [], None
        for _, r in df.iterrows():
            if prev and r['Date'] != prev:
                rows.append({'Date':None,'Not in account statement':None,'Not in our books':None})
            rows.append(r.to_dict())
            prev = r['Date']
            
        final = pd.DataFrame(rows)
        final[''] = None
        final = final[['Date','Not in account statement','Credit','','Not in our books','Given']]
        return final
        
    except Exception as e:
        print(f"Error in process_payments: {str(e)}")
        # Return empty DataFrame with correct columns if processing fails
        return pd.DataFrame(columns=['Date','Not in account statement','Credit','','Not in our books','Given'])

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process_files():
    try:
        if 'ac_statement' not in request.files or 'our_books' not in request.files:
            return "Error: Both files are required!"
            
        ac_file, ob_file = request.files['ac_statement'], request.files['our_books']
        
        # Create a temporary directory for this request
        temp_dir = tempfile.mkdtemp(dir=app.config['UPLOAD_FOLDER'])
        
        # Save files to the temporary directory
        ac_path = os.path.join(temp_dir, secure_filename(ac_file.filename))
        ob_path = os.path.join(temp_dir, secure_filename(ob_file.filename))
        
        ac_file.save(ac_path)
        ob_file.save(ob_path)
        
        timestamp = int(time.time())
        out = os.path.join(temp_dir, f"combined_{timestamp}.xlsx")
        
        # Process with timeout handling
        recv_df = process_receivements(ac_path, ob_path, timestamp)
        pay_df = process_payments(ac_path, ob_path, timestamp)
        
        # Use context manager for file operations
        with pd.ExcelWriter(out, engine='openpyxl') as w:
            recv_df.to_excel(w, sheet_name='Receivements', index=False)
            pay_df.to_excel(w, sheet_name='Payments', index=False)
        
        # Optional styling across both sheets - with memory optimization
        wb = load_workbook(out)
        grey = PatternFill(start_color="DDDDDD", end_color="DDDDDD", fill_type="solid")
        
        for ws in wb.worksheets:
            for i in range(2, ws.max_row+1):
                if ws[f"A{i}"].value is None:
                    for col in ws.iter_cols(min_col=1, max_col=6, min_row=i, max_row=i):
                        col[0].fill = grey
        
        wb.save(out)
        
        # Clean up individual files to save memory
        try:
            os.remove(ac_path)
            os.remove(ob_path)
        except:
            pass  # Don't fail if cleanup doesn't work
            
        # Create a copy of the output file that will survive beyond this function
        final_out = os.path.join(app.config['UPLOAD_FOLDER'], f"combined_{timestamp}.xlsx")
        with open(out, 'rb') as src_file, open(final_out, 'wb') as dst_file:
            dst_file.write(src_file.read())
            
        # Clean up temp directory after processing
        try:
            for f in os.listdir(temp_dir):
                os.remove(os.path.join(temp_dir, f))
            os.rmdir(temp_dir)
        except:
            pass  # Don't fail if cleanup doesn't work
            
        return send_file(final_out, as_attachment=True, download_name=f"combined_{timestamp}.xlsx")
        
    except Exception as e:
        print(f"Error in process_files: {str(e)}")
        return f"An error occurred during processing: {str(e)}", 500

@app.route('/health')
def health_check():
    # Add a health check endpoint for monitoring
    return "OK", 200

if __name__ == '__main__':
    # Add timeout configuration
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)), threaded=True)
