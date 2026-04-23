"""
Process C1H workflow results: Convert JSON to CSV and split PDFs
"""
import json
import csv
import os
import re
import sys
import glob

sys.stdout.reconfigure(encoding='utf-8')

try:
    import fitz  # PyMuPDF
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    print("Note: PyMuPDF not installed. PDF splitting will be skipped.")

# ============================================================
# CONFIGURATION
# ============================================================
JSON_FILE = "C1H_workflow_results.json"
CSV_OUTPUT = "C1H_papers_combined.csv"
PAPERS_FOLDER = "C1H Papers and Mark Schemes"

# ============================================================
# LOAD JSON DATA
# ============================================================
print("=" * 60)
print("LOADING JSON DATA")
print("=" * 60)

with open(JSON_FILE, 'r', encoding='utf-8') as f:
    all_results = json.load(f)

print(f"Found {len(all_results)} paper results")

# ============================================================
# CONVERT TO CSV
# ============================================================
print("\n" + "=" * 60)
print("CONVERTING TO CSV")
print("=" * 60)

# CSV columns (standard order)
fieldnames = [
    'session', 'exam', 'year', 'question_id', 'subquestion_id', 'type',
    'marks', 'AO', 'spec_reference', 'question_text', 'mark_scheme', 'extra_notes',
    'question_page_start', 'question_page_end', 'mark_scheme_start_page', 'mark_scheme_end_page',
    'has_figure', 'figure_labels_joined', 'has_table', 'table_labels_joined',
    'source_qp', 'source_ms'
]

all_questions = []

with open(CSV_OUTPUT, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
    writer.writeheader()
    
    for paper in all_results:
        session = paper.get('session', '')
        source_qp = paper.get('qp_file', '')
        source_ms = paper.get('ms_file', '')
        
        # Extract questions from nested structure
        result = paper.get('result', {})
        outputs = result.get('data', {}).get('outputs', {})
        output = outputs.get('output', [])
        
        questions = []
        if output and len(output) > 0 and len(output[0]) > 0:
            json_data = output[0][0].get('JSON', {})
            questions = json_data.get('questions', [])
        
        print(f"  {session}: {len(questions)} questions")
        
        for q in questions:
            row = {
                'session': session,
                'exam': q.get('exam', ''),
                'year': q.get('year', ''),
                'question_id': q.get('question_id', ''),
                'subquestion_id': q.get('subquestion_id', ''),
                'type': q.get('type', ''),
                'marks': q.get('marks', ''),
                'AO': q.get('AO', ''),
                'spec_reference': q.get('spec_reference', ''),
                'question_text': q.get('question_text', ''),
                'mark_scheme': q.get('mark_scheme', ''),
                'extra_notes': q.get('extra_notes', ''),
                'question_page_start': q.get('question_page_start', ''),
                'question_page_end': q.get('question_page_end', ''),
                'mark_scheme_start_page': q.get('mark_scheme_start_page', ''),
                'mark_scheme_end_page': q.get('mark_scheme_end_page', ''),
                'has_figure': q.get('has_figure', ''),
                'figure_labels_joined': '; '.join(q.get('figure_labels', [])),
                'has_table': q.get('has_table', ''),
                'table_labels_joined': '; '.join(q.get('table_labels', [])),
                'source_qp': source_qp,
                'source_ms': source_ms
            }
            writer.writerow(row)
            
            # Store for PDF splitting
            all_questions.append({
                'session': session,
                'subquestion_id': q.get('subquestion_id', ''),
                'question_page_start': q.get('question_page_start'),
                'question_page_end': q.get('question_page_end'),
                'source_qp': source_qp
            })

print(f"\nCSV saved: {CSV_OUTPUT}")
print(f"Total rows: {len(all_questions)}")

# ============================================================
# SPLIT PDFs
# ============================================================
if PDF_AVAILABLE and all_questions:
    print("\n" + "=" * 60)
    print("SPLITTING PDFs")
    print("=" * 60)
    
    # Create output folder
    output_folder = "8464C1H_question_pdfs"
    os.makedirs(output_folder, exist_ok=True)
    print(f"Output folder: {output_folder}")
    
    # Group questions by source PDF
    pdf_questions = {}
    for q in all_questions:
        qp = q['source_qp']
        if qp not in pdf_questions:
            pdf_questions[qp] = []
        pdf_questions[qp].append(q)
    
    pdf_count = 0
    
    for pdf_path, qs in pdf_questions.items():
        # Find the actual PDF file
        if os.path.exists(pdf_path):
            actual_path = pdf_path
        else:
            # Search in papers folder
            basename = os.path.basename(pdf_path)
            matches = glob.glob(os.path.join(PAPERS_FOLDER, '*'))
            found = None
            for m in matches:
                if basename.lower() in os.path.basename(m).lower():
                    found = m
                    break
            if found:
                actual_path = found
            else:
                print(f"  WARNING: Could not find PDF: {pdf_path}")
                continue
        
        print(f"\nProcessing: {os.path.basename(actual_path)}")
        
        try:
            doc = fitz.open(actual_path)
            session = qs[0]['session'] if qs else ''
            
            for q in qs:
                q_id = q['subquestion_id']
                start_page = q['question_page_start']
                end_page = q['question_page_end']
                
                if start_page is None or end_page is None:
                    continue
                
                start_idx = int(start_page) - 1
                end_idx = int(end_page) - 1
                
                if start_idx < 0 or end_idx >= len(doc) or start_idx > end_idx:
                    continue
                
                new_doc = fitz.open()
                new_doc.insert_pdf(doc, from_page=start_idx, to_page=end_idx)
                
                # Generate filename: AQA-8464C1H-JUN18_Q01_1.pdf
                safe_q_id = q_id.replace('.', '_')
                base_name = os.path.basename(actual_path).replace('-QP', '').replace('.PDF', '').replace('.pdf', '')
                filename = f"{base_name}_Q{safe_q_id}.pdf"
                filepath = os.path.join(output_folder, filename)
                
                new_doc.save(filepath)
                new_doc.close()
                pdf_count += 1
            
            doc.close()
            
        except Exception as e:
            print(f"  Error: {e}")
    
    print(f"\n✓ Created {pdf_count} question PDFs in '{output_folder}'")

print("\n" + "=" * 60)
print("COMPLETE!")
print("=" * 60)

