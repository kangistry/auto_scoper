"""
Script to automatically add PDF links to the questions spreadsheet.
Matches each question row to the appropriate PDF file based on paper, session, and question number.
"""

import pandas as pd
import os
import re
from pathlib import Path

# Configuration
CSV_FILE = "ALL_PAPERS_COMBINED.csv"
OUTPUT_FILE = "ALL_PAPERS_COMBINED_WITH_LINKS.csv"

# PDF folders to scan
PDF_FOLDERS = [
    "8464C1F_question_pdfs",
    "8464C1H_question_pdfs", 
    "8464C2F_question_pdfs",
    "8464C2H_question_pdfs",
]

def get_script_dir():
    """Get the directory where this script is located."""
    return Path(__file__).parent.resolve()

def scan_pdf_folders(base_dir):
    """
    Scan all PDF folders and build an index of available PDFs.
    Returns a dict mapping (paper_code, session, question_num, subq_num) -> file_path
    """
    pdf_index = {}
    
    for folder in PDF_FOLDERS:
        folder_path = base_dir / folder
        if not folder_path.exists():
            print(f"Warning: Folder {folder} not found, skipping...")
            continue
            
        # Extract paper code from folder name (e.g., "8464C1F" from "8464C1F_question_pdfs")
        paper_code = folder.replace("_question_pdfs", "")
        
        for pdf_file in folder_path.glob("*.pdf"):
            filename = pdf_file.name
            
            # Parse filename patterns like:
            # AQA-8464C1F-JUN22_Q01_1.pdf (per-subquestion)
            # AQA-8464C1F-NOV20_Q01.pdf (whole question)
            # NOV21_Q01.pdf (simplified naming)
            # AQA-8464C1H-JUN19_QQ01_1.pdf (double Q typo)
            
            # Try to extract session, question number, and optional subquestion
            # Pattern for standard naming: AQA-8464XXX-SESSION[-CR]_Q##[_#].pdf
            # The -CR suffix is optional (used for some papers like C2F JUN22)
            match = re.search(r'(?:AQA-\d+C\d[FH]-)?([A-Z]{3}\d{2})(?:-CR)?_Q+(\d+)(?:_(\d+))?\.pdf', filename, re.IGNORECASE)
            
            if match:
                session = match.group(1).upper()  # e.g., "JUN22", "NOV20"
                question_num = int(match.group(2))  # e.g., 1, 2, 3
                subq_num = int(match.group(3)) if match.group(3) else None  # e.g., 1, 2, None
                
                key = (paper_code, session, question_num, subq_num)
                pdf_index[key] = str(pdf_file)
                
    print(f"Indexed {len(pdf_index)} PDF files")
    return pdf_index

def extract_paper_info(source_qp):
    """
    Extract paper code and session from source_qp path.
    e.g., "C1F Papers and Mark Schemes\\AQA-8464C1F-QP-NOV20.PDF" -> ("8464C1F", "NOV20")
    """
    if pd.isna(source_qp):
        return None, None
    
    # Try to extract from the filename
    match = re.search(r'8464(C\d[FH])', source_qp)
    paper_code = f"8464{match.group(1)}" if match else None
    
    # Extract session (e.g., NOV20, JUN22)
    session_match = re.search(r'(NOV|JUN|OCT)(\d{2})', source_qp, re.IGNORECASE)
    session = f"{session_match.group(1).upper()}{session_match.group(2)}" if session_match else None
    
    return paper_code, session

def extract_subquestion_number(subquestion_id):
    """
    Extract the subquestion number from subquestion_id.
    Handles multiple formats:
    - "Q01.1" -> 1
    - "1.1" -> 1  
    - "Q01.2" -> 2
    - "2.3" -> 3
    """
    if pd.isna(subquestion_id):
        return None
    
    subq_str = str(subquestion_id)
    
    # Try "Q##.#" format first
    match = re.search(r'Q\d+\.(\d+)', subq_str)
    if match:
        return int(match.group(1))
    
    # Try "#.#" format (e.g., "1.1", "2.3")
    match = re.search(r'\d+\.(\d+)', subq_str)
    if match:
        return int(match.group(1))
    
    return None

def find_pdf_for_row(row, pdf_index):
    """
    Find the best matching PDF for a given row.
    First tries to find a per-subquestion PDF, then falls back to whole-question PDF.
    """
    paper_code, session = extract_paper_info(row['source_qp'])
    question_num = row['question_id']
    subq_num = extract_subquestion_number(row['subquestion_id'])
    
    if not paper_code or not session or pd.isna(question_num):
        return None
    
    question_num = int(question_num)
    
    # First try: exact match with subquestion
    if subq_num:
        key = (paper_code, session, question_num, subq_num)
        if key in pdf_index:
            return pdf_index[key]
    
    # Second try: whole question PDF (no subquestion suffix)
    key = (paper_code, session, question_num, None)
    if key in pdf_index:
        return pdf_index[key]
    
    return None

def make_relative_path(absolute_path, base_dir):
    """Convert absolute path to relative path from base directory."""
    if absolute_path is None:
        return None
    try:
        return str(Path(absolute_path).relative_to(base_dir))
    except ValueError:
        return absolute_path

def main():
    base_dir = get_script_dir()
    
    print(f"Working directory: {base_dir}")
    print(f"Reading CSV: {CSV_FILE}")
    
    # Read CSV
    df = pd.read_csv(base_dir / CSV_FILE)
    print(f"Loaded {len(df)} rows")
    
    # Scan PDF folders
    pdf_index = scan_pdf_folders(base_dir)
    
    # Find PDF for each row
    print("Matching PDFs to questions...")
    pdf_links = []
    matched_count = 0
    
    for idx, row in df.iterrows():
        pdf_path = find_pdf_for_row(row, pdf_index)
        if pdf_path:
            # Convert to relative path for portability
            relative_path = make_relative_path(pdf_path, base_dir)
            pdf_links.append(relative_path)
            matched_count += 1
        else:
            pdf_links.append(None)
    
    # Add new column
    df['question_pdf_link'] = pdf_links
    
    # Save output
    output_path = base_dir / OUTPUT_FILE
    df.to_csv(output_path, index=False)
    
    print(f"\nResults:")
    print(f"  Total rows: {len(df)}")
    print(f"  Matched: {matched_count}")
    print(f"  Unmatched: {len(df) - matched_count}")
    print(f"\nSaved to: {output_path}")
    
    # Show some examples of unmatched rows for debugging
    unmatched = df[df['question_pdf_link'].isna()]
    if len(unmatched) > 0:
        print(f"\nSample unmatched rows (first 5):")
        for idx, row in unmatched.head(5).iterrows():
            print(f"  Session: {row.get('session')}, Q{row.get('question_id')}.{row.get('subquestion_id')}")
            print(f"    source_qp: {row.get('source_qp')}")

if __name__ == "__main__":
    main()
