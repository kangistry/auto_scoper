import fitz  # PyMuPDF
import os
import json
from pathlib import Path

def find_pdf_file(filename):
    """Search for a PDF file in current directory and common subfolders."""
    import glob
    
    # First try the exact path
    if os.path.exists(filename):
        return filename
    
    # Try just the basename in current directory
    basename = os.path.basename(filename)
    if os.path.exists(basename):
        return basename
    
    # Search in subfolders that might contain papers
    search_patterns = [
        f"**/{basename}",
        f"*Papers*/{basename}",
        f"*Mark*/{basename}",
    ]
    
    for pattern in search_patterns:
        matches = glob.glob(pattern, recursive=True)
        if matches:
            return matches[0]
    
    return None


def batch_split_pdfs_by_questions(json_path, output_folder=None):
    """
    Process multiple exam PDFs and split them by main questions.
    Works with the new workflow_results_all.json format.
    Names PDFs based on source filename: AQA-8464C2H-JUN18_Q01.pdf
    Output folder derived from exam code: 8464C2H_question_pdfs/
    """
    import re
    
    # Load the JSON array
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    # Extract all questions from the new nested structure
    all_questions = []
    session_to_pdf = {}  # Map session to PDF filename
    
    for session_data in data:
        session = session_data.get('session', 'Unknown')
        qp_file = session_data.get('qp_file', '')
        
        # Store the mapping from session to PDF
        session_to_pdf[session] = qp_file
        
        # Navigate the nested structure
        try:
            result = session_data.get('result', {})
            outputs = result.get('data', {}).get('outputs', {}).get('output', [])
            
            for output_group in outputs:
                for output_item in output_group:
                    questions = output_item.get('JSON', {}).get('questions', [])
                    
                    for q in questions:
                        # Add session info to help map to PDF
                        q['_session'] = session
                        q['_source_pdf'] = qp_file
                        all_questions.append(q)
                        
        except Exception as e:
            print(f"  Warning: Error extracting from {session} - {e}")
    
    # Group questions by session and main question_id
    exams = {}  # {session: {question_id: [subquestions]}}
    
    for q in all_questions:
        session = q.get("_session", "Unknown")
        q_id = q.get("question_id", "Q?")
        
        if session not in exams:
            exams[session] = {}
        if q_id not in exams[session]:
            exams[session][q_id] = []
        
        exams[session][q_id].append(q)
    
    print(f"Found {len(all_questions)} total questions")
    print(f"Across {len(exams)} exam sessions")
    print("=" * 60)
    
    # Extract exam code from first PDF filename for folder name
    # e.g., "AQA-8464C2H-QP-JUN18.PDF" -> "8464C2H"
    first_pdf = list(session_to_pdf.values())[0] if session_to_pdf else ""
    exam_code_match = re.search(r'(\d{4}[A-Z]\d?[A-Z]?)', first_pdf)
    exam_code = exam_code_match.group(1) if exam_code_match else "exam"
    
    # Use provided output_folder or generate from exam code
    if output_folder is None:
        output_folder = f"{exam_code}_question_pdfs"
    
    # Create output folder
    os.makedirs(output_folder, exist_ok=True)
    
    total_pdfs_created = 0
    
    # Process each session
    for session in sorted(exams.keys()):
        # Get the PDF filename from our mapping
        pdf_path_original = session_to_pdf.get(session, '')
        
        if not pdf_path_original:
            print(f"\nWarning: No PDF mapped for session {session}, skipping...")
            continue
        
        # Try to find the PDF (may have been moved to subfolder)
        pdf_path = find_pdf_file(pdf_path_original)
        
        if not pdf_path:
            print(f"\nWarning: PDF not found: {pdf_path_original}, skipping...")
            continue
        
        # Extract base name from PDF for file naming
        # e.g., "AQA-8464C2H-QP-JUN18.PDF" -> "AQA-8464C2H-JUN18"
        pdf_basename = os.path.splitext(os.path.basename(pdf_path))[0]
        # Remove "-QP" or "-W-MS" etc. to get cleaner name
        clean_basename = re.sub(r'-QP|-W?-?MS', '', pdf_basename)
        
        print(f"\n{'='*60}")
        print(f"Processing {session} -> {clean_basename}")
        print(f"Source: {pdf_path}")
        print(f"Found {len(exams[session])} main questions")
        print('-' * 60)
        
        # Open the PDF
        pdf_document = fitz.open(pdf_path)
        
        # Process each main question for this session
        for idx, (question_id, subquestions) in enumerate(sorted(exams[session].items()), 1):
            # Find page range for all subquestions
            page_starts = [sq.get("question_page_start") or sq.get("page_start") for sq in subquestions 
                          if sq.get("question_page_start") or sq.get("page_start")]
            page_ends = [sq.get("question_page_end") or sq.get("page_end") for sq in subquestions 
                        if sq.get("question_page_end") or sq.get("page_end")]
            
            if not page_starts or not page_ends:
                print(f"  Warning: Q{question_id} missing page info, skipping")
                continue
            
            page_start = min(page_starts)
            page_end = max(page_ends)
            
            # Create new PDF for this question
            output_pdf = fitz.open()
            
            # Copy pages (0-indexed in fitz)
            for page_num in range(page_start - 1, page_end):
                if page_num < len(pdf_document):
                    output_pdf.insert_pdf(pdf_document, from_page=page_num, to_page=page_num)
            
            # Generate filename: AQA-8464C2H-JUN18_Q01.pdf
            q_id_str = f"Q{int(question_id):02d}" if isinstance(question_id, int) else f"Q{question_id}"
            filename = f"{clean_basename}_{q_id_str}.pdf"
            filepath = os.path.join(output_folder, filename)
            
            # Save the PDF
            output_pdf.save(filepath)
            output_pdf.close()
            
            # Get subquestion IDs for display
            subq_ids = [sq.get("subquestion_id", "?") for sq in subquestions]
            page_range = f"p{page_start}" if page_start == page_end else f"p{page_start}-{page_end}"
            
            print(f"  {idx:2d}. {filename:35s} ({page_range:8s}) - {len(subquestions)} parts")
            
            total_pdfs_created += 1
        
        pdf_document.close()
    
    print("\n" + "=" * 60)
    print(f"Batch processing complete!")
    print(f"  Total question PDFs created: {total_pdfs_created}")
    print(f"  Saved to: {os.path.abspath(output_folder)}")
    
    return total_pdfs_created


if __name__ == "__main__":
    json_file = "workflow_results_all.json"
    
    if not os.path.exists(json_file):
        print(f"Error: File '{json_file}' not found!")
        exit(1)
    
    print(f"Reading exam data from: {json_file}")
    print("Output folder will be auto-generated from exam code")
    print("=" * 60)
    
    batch_split_pdfs_by_questions(json_file)
    
    print("\n" + "=" * 60)
    print("Done! All question PDFs are ready.")

