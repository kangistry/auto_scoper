"""
Split mark scheme PDFs by question (not subquestion).
Creates one PDF per question containing the full mark scheme for that question.
"""
import pandas as pd
import fitz  # PyMuPDF
import os
import re
from collections import defaultdict

# Configuration
CSV_FILE = "BACKUP COPY AQA GCSE Biology Papers - SEPARATE SCIENCE PAPERS.csv"
MS_FOLDER = "Mark Schemes"
OUTPUT_FOLDER = "Mark_Scheme_PDFs_by_Question"

def extract_paper_and_session(source_ms):
    """Extract paper code and session from source_ms filename."""
    if not source_ms or pd.isna(source_ms):
        return None, None
    
    # Match patterns like AQA-84611H-MS-JUN22.PDF or AQA-84611H-W-MS-JUN18.PDF
    match = re.search(r'AQA-(\d{5}[FH])', source_ms, re.IGNORECASE)
    paper_code = match.group(1) if match else None
    
    # Extract session (JUN18, NOV20, etc.)
    session_match = re.search(r'(JUN|NOV|OCT)\d{2}', source_ms, re.IGNORECASE)
    session = session_match.group(0).upper() if session_match else None
    
    return paper_code, session

def find_ms_file(source_ms, ms_folder):
    """Find the actual mark scheme file, handling naming variations."""
    if not source_ms or pd.isna(source_ms):
        return None
    
    # Try exact match first
    exact_path = os.path.join(ms_folder, source_ms)
    if os.path.exists(exact_path):
        return exact_path
    
    # Get paper code and session for fuzzy matching
    paper_code, session = extract_paper_and_session(source_ms)
    if not paper_code or not session:
        return None
    
    # Look for matching file in folder
    for f in os.listdir(ms_folder):
        if paper_code in f.upper() and session in f.upper() and f.lower().endswith('.pdf'):
            return os.path.join(ms_folder, f)
    
    return None

def main():
    print("=" * 60)
    print("MARK SCHEME SPLITTER BY QUESTION")
    print("=" * 60)
    
    # Load CSV
    print(f"\nLoading CSV: {CSV_FILE}")
    df = pd.read_csv(CSV_FILE)
    print(f"Total rows: {len(df)}")
    
    # Check required columns
    required_cols = ['source_ms', 'question_id', 'mark_scheme_start_page', 'mark_scheme_end_page']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"ERROR: Missing columns: {missing}")
        return
    
    # Create output folder
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
    # Group by source_ms and question_id to get page ranges
    # For each (source_ms, question_id), we want the min start page and max end page
    print("\nGrouping questions by mark scheme...")
    
    question_pages = defaultdict(lambda: {'start': float('inf'), 'end': 0})
    
    for _, row in df.iterrows():
        source_ms = row.get('source_ms', '')
        q_id = row.get('question_id', '')
        start_page = row.get('mark_scheme_start_page')
        end_page = row.get('mark_scheme_end_page')
        
        if pd.isna(source_ms) or pd.isna(q_id) or pd.isna(start_page) or pd.isna(end_page):
            continue
        
        key = (source_ms, int(q_id))
        question_pages[key]['start'] = min(question_pages[key]['start'], int(start_page))
        question_pages[key]['end'] = max(question_pages[key]['end'], int(end_page))
    
    print(f"Found {len(question_pages)} unique (mark_scheme, question) combinations")
    
    # Group by source_ms for processing
    ms_questions = defaultdict(list)
    for (source_ms, q_id), pages in question_pages.items():
        ms_questions[source_ms].append({
            'question_id': q_id,
            'start': pages['start'],
            'end': pages['end']
        })
    
    print(f"Found {len(ms_questions)} unique mark scheme files to process")
    
    # Process each mark scheme
    total_pdfs = 0
    errors = []
    
    for source_ms, questions in ms_questions.items():
        print(f"\n--- Processing: {source_ms} ---")
        
        # Find the actual file
        ms_path = find_ms_file(source_ms, MS_FOLDER)
        if not ms_path:
            print(f"  WARNING: File not found for {source_ms}")
            errors.append(f"File not found: {source_ms}")
            continue
        
        print(f"  Found: {os.path.basename(ms_path)}")
        
        # Extract paper code and session for output naming
        paper_code, session = extract_paper_and_session(source_ms)
        if not paper_code or not session:
            print(f"  WARNING: Could not parse paper/session from {source_ms}")
            errors.append(f"Could not parse: {source_ms}")
            continue
        
        try:
            pdf_doc = fitz.open(ms_path)
            total_pages = len(pdf_doc)
            print(f"  Total pages in PDF: {total_pages}")
            
            # Sort questions by ID
            questions.sort(key=lambda x: x['question_id'])
            
            for q in questions:
                q_id = q['question_id']
                start_page = q['start']
                end_page = q['end']
                
                # Validate page range (1-indexed in CSV, 0-indexed in PyMuPDF)
                if start_page < 1 or end_page > total_pages:
                    print(f"  WARNING: Invalid page range for Q{q_id}: {start_page}-{end_page} (PDF has {total_pages} pages)")
                    continue
                
                # Create output filename
                output_name = f"AQA-{paper_code}-{session}-MS-Q{q_id}.pdf"
                output_path = os.path.join(OUTPUT_FOLDER, output_name)
                
                # Extract pages (convert to 0-indexed)
                output_pdf = fitz.open()
                for page_num in range(start_page - 1, end_page):
                    output_pdf.insert_pdf(pdf_doc, from_page=page_num, to_page=page_num)
                
                output_pdf.save(output_path)
                output_pdf.close()
                
                print(f"  Created: {output_name} (pages {start_page}-{end_page})")
                total_pdfs += 1
            
            pdf_doc.close()
            
        except Exception as e:
            print(f"  ERROR processing {source_ms}: {e}")
            errors.append(f"Error with {source_ms}: {e}")
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total PDFs created: {total_pdfs}")
    print(f"Output folder: {OUTPUT_FOLDER}")
    
    if errors:
        print(f"\nErrors ({len(errors)}):")
        for e in errors:
            print(f"  - {e}")
    
    print("\nDone!")

if __name__ == "__main__":
    main()
