"""
Process a single paper result - convert to CSV and split PDFs
"""
import json
import csv
import os
import glob
import re

# Try to import fitz for PDF splitting
try:
    import fitz
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    print("Note: PyMuPDF not installed - PDF splitting will be skipped")

# ============================================================
# CONFIGURATION
# ============================================================
INPUT_JSON = "C1F_JUN22_result.json"
OUTPUT_CSV = "C1F_JUN22_papers.csv"
OUTPUT_PDF_FOLDER = "8464C1F_question_pdfs"
# ============================================================

def find_pdf_file(filename):
    """Search for a PDF file in current directory and subfolders."""
    if os.path.exists(filename):
        return filename
    
    basename = os.path.basename(filename)
    if os.path.exists(basename):
        return basename
    
    # Search in subfolders
    for pattern in [f"**/{basename}", f"*Papers*/{basename}"]:
        matches = glob.glob(pattern, recursive=True)
        if matches:
            return matches[0]
    return None


def convert_to_csv(json_file, csv_file):
    """Convert JSON result to CSV."""
    print(f"\nConverting {json_file} to CSV...")
    
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    all_questions = []
    
    for session_data in data:
        session = session_data.get('session', 'Unknown')
        qp_file = session_data.get('qp_file', '')
        ms_file = session_data.get('ms_file', '')
        
        try:
            result = session_data.get('result', {})
            outputs = result.get('data', {}).get('outputs', {}).get('output', [])
            
            for output_group in outputs:
                for output_item in output_group:
                    questions = output_item.get('JSON', {}).get('questions', [])
                    
                    for q in questions:
                        q['session'] = session
                        q['source_qp'] = qp_file
                        q['source_ms'] = ms_file
                        all_questions.append(q)
                        
            print(f"  [OK] {session}: Extracted {len(questions)} questions")
            
        except Exception as e:
            print(f"  [ERROR] {session}: {e}")
    
    # Write CSV
    fieldnames = [
        'session', 'exam', 'year', 'question_id', 'subquestion_id', 'type', 'marks',
        'AO', 'spec_reference', 'question_text', 'mark_scheme', 'extra_notes',
        'question_page_start', 'question_page_end', 'mark_scheme_start_page', 'mark_scheme_end_page',
        'has_figure', 'figure_labels_joined', 'has_table', 'table_labels_joined',
        'source_qp', 'source_ms'
    ]
    
    with open(csv_file, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        
        for question in all_questions:
            row = {}
            for key in fieldnames:
                if key == 'figure_labels_joined':
                    value = question.get('figure_labels', [])
                elif key == 'table_labels_joined':
                    value = question.get('table_labels', [])
                else:
                    value = question.get(key, '')
                
                if isinstance(value, list):
                    value = ', '.join(str(item) for item in value) if value else ''
                elif isinstance(value, bool):
                    value = str(value).lower()
                
                row[key] = value
            
            writer.writerow(row)
    
    print(f"\n  Total questions: {len(all_questions)}")
    print(f"  CSV saved to: {csv_file}")
    
    return all_questions


def split_pdfs(json_file, output_folder):
    """Split PDFs by question."""
    if not PDF_AVAILABLE:
        print("\nSkipping PDF splitting (PyMuPDF not installed)")
        return
    
    print(f"\nSplitting PDFs...")
    
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    os.makedirs(output_folder, exist_ok=True)
    
    all_questions = []
    session_to_pdf = {}
    
    for session_data in data:
        session = session_data.get('session', 'Unknown')
        qp_file = session_data.get('qp_file', '')
        session_to_pdf[session] = qp_file
        
        try:
            result = session_data.get('result', {})
            outputs = result.get('data', {}).get('outputs', {}).get('output', [])
            
            for output_group in outputs:
                for output_item in output_group:
                    questions = output_item.get('JSON', {}).get('questions', [])
                    for q in questions:
                        q['_session'] = session
                        all_questions.append(q)
        except:
            pass
    
    # Group by session and question_id
    exams = {}
    for q in all_questions:
        session = q.get("_session", "Unknown")
        q_id = q.get("question_id", "Q?")
        
        if session not in exams:
            exams[session] = {}
        if q_id not in exams[session]:
            exams[session][q_id] = []
        exams[session][q_id].append(q)
    
    total_pdfs = 0
    
    for session in exams:
        pdf_path_original = session_to_pdf.get(session, '')
        pdf_path = find_pdf_file(pdf_path_original)
        
        if not pdf_path:
            print(f"  [SKIP] {session}: PDF not found")
            continue
        
        # Get clean basename for output files
        pdf_basename = os.path.splitext(os.path.basename(pdf_path))[0]
        clean_basename = re.sub(r'-QP|-W?-?MS', '', pdf_basename)
        
        print(f"  Processing {session} -> {clean_basename}...")
        
        pdf_document = fitz.open(pdf_path)
        
        for question_id, subquestions in sorted(exams[session].items()):
            page_starts = [sq.get("question_page_start") or sq.get("page_start") for sq in subquestions 
                          if sq.get("question_page_start") or sq.get("page_start")]
            page_ends = [sq.get("question_page_end") or sq.get("page_end") for sq in subquestions 
                        if sq.get("question_page_end") or sq.get("page_end")]
            
            if not page_starts or not page_ends:
                continue
            
            page_start = min(page_starts)
            page_end = max(page_ends)
            
            output_pdf = fitz.open()
            
            for page_num in range(page_start - 1, page_end):
                if page_num < len(pdf_document):
                    output_pdf.insert_pdf(pdf_document, from_page=page_num, to_page=page_num)
            
            q_id_str = f"Q{int(question_id):02d}" if isinstance(question_id, int) else f"Q{question_id}"
            filename = f"{clean_basename}_{q_id_str}.pdf"
            filepath = os.path.join(output_folder, filename)
            
            output_pdf.save(filepath)
            output_pdf.close()
            
            print(f"    Created: {filename}")
            total_pdfs += 1
        
        pdf_document.close()
    
    print(f"\n  Total PDFs created: {total_pdfs}")
    print(f"  Saved to: {output_folder}")


if __name__ == "__main__":
    print("=" * 60)
    print("SINGLE RESULT PROCESSOR")
    print("=" * 60)
    print(f"Input: {INPUT_JSON}")
    print(f"CSV Output: {OUTPUT_CSV}")
    print(f"PDF Output: {OUTPUT_PDF_FOLDER}")
    
    if not os.path.exists(INPUT_JSON):
        print(f"\nError: {INPUT_JSON} not found!")
        exit(1)
    
    # Step 1: Convert to CSV
    print("\n" + "=" * 60)
    print("STEP 1: CONVERTING TO CSV")
    print("=" * 60)
    convert_to_csv(INPUT_JSON, OUTPUT_CSV)
    
    # Step 2: Split PDFs
    print("\n" + "=" * 60)
    print("STEP 2: SPLITTING PDFs")
    print("=" * 60)
    split_pdfs(INPUT_JSON, OUTPUT_PDF_FOLDER)
    
    print("\n" + "=" * 60)
    print("DONE!")
    print("=" * 60)

