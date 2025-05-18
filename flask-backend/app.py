from flask import Flask, render_template, request, send_file
import pandas as pd
import re
import os
import time

app = Flask(__name__)

# Function to process the Excel files
def process_account_statement(ac_statement_file, our_books_file):
    try:
        # Determine file formats and set engine
        ac_statement_engine = 'openpyxl' if ac_statement_file.filename.endswith('.xlsx') else 'xlrd'
        our_books_engine = 'openpyxl' if our_books_file.filename.endswith('.xlsx') else 'xlrd'

        # Load account statement file
        ac_statement = (
            pd.read_excel(ac_statement_file, header=None, engine=ac_statement_engine)
            .drop(index=range(15))  # Drop the first 15 rows
            .dropna(axis=1, how='all')  # Drop columns where all values are NaN
            .reset_index(drop=True)  # Reset the index
        )
        ac_statement.columns = ['Date', 'Particular', 'Given', 'Received', 'Balance']

        # Load our books file
        our_books = (
            pd.read_excel(our_books_file, engine=our_books_engine)
            .dropna(axis=1, how='all')
            .drop(index=[0,1])  # Drop the first row
            .reset_index(drop=True)
        )

        # Extract relevant payment records
        payments_ac_statement = ac_statement[ac_statement['Given'].notna()].copy()
        payments_our_books = our_books[our_books['Credit'] != 0].copy()
        payments_our_books['Particular'] = payments_our_books['Particular'].str.strip()

        # Define helper functions for extracting names
        def extract_name(description):
            return re.sub(r"(NEFT-|MClick/To\s+)", "", str(description)).strip()

        def extract_supplier_name(transaction):
            parts = str(transaction).split('/')
            return parts[0].strip() if len(parts) >= 2 else transaction.strip()

        # Apply transformations
        payments_ac_statement['Particular'] = payments_ac_statement['Particular'].apply(extract_name)
        payments_ac_statement['SupplierName'] = payments_ac_statement['Particular'].apply(extract_supplier_name)

        # Identify discrepancies
        ac_statement_set = set(payments_ac_statement['SupplierName'].unique())
        our_books_set = set(payments_our_books['Particular'].unique())

        unique_to_ac_statement = list(ac_statement_set - our_books_set)
        unique_to_our_books = list(our_books_set - ac_statement_set)

        # Ensure lists are of the same length
        max_length = max(len(unique_to_ac_statement), len(unique_to_our_books))
        unique_to_ac_statement += [None] * (max_length - len(unique_to_ac_statement))
        unique_to_our_books += [None] * (max_length - len(unique_to_our_books))

        # Create discrepancies DataFrame
        discrepancies = pd.DataFrame({
            'Not present in payments from account statement': unique_to_our_books,
            'Not present in payments from our books': unique_to_ac_statement
        })

        return discrepancies
    except Exception as e:
        print(f"Error occurred: {e}")  # Log the error
        return "An error occurred while processing the files. Please check the input files and try again."

# Home route
@app.route('/')
def home():
    return render_template('index.html')

# Route to handle file uploads and processing
@app.route('/process', methods=['POST'])
def process_files():
    if 'ac_statement' not in request.files or 'our_books' not in request.files:
        return "Error: Both files are required!"

    ac_statement_file = request.files['ac_statement']
    our_books_file = request.files['our_books']

    discrepancies = process_account_statement(ac_statement_file, our_books_file)

    if isinstance(discrepancies, str):  # If an error occurred
        return discrepancies

    # Save discrepancies to Excel with a unique filename
    timestamp = int(time.time())
    output_file = f"discrepancies_{timestamp}.xlsx"
    discrepancies.to_excel(output_file, index=False)

    return send_file(output_file, as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv("PORT", 5000)))
