import fitz  # PyMuPDF
import os
import json
from pathlib import Path

def batch_split_pdfs_by_questions(json_path, output_folder="all_question_pdfs"):
    """
    Process multiple exam PDFs and split them by main questions.
    Names PDFs as: YEAR_QXX.pdf (e.g., 2022_Q01.pdf, 2024_Q03.pdf)
    """
    # Load the JSON array
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    # Extract all questions from the nested structure
    all_questions = []
    output_array = data.get("output", [])
    
    for exam_group in output_array:
        for exam_data in exam_group:
            json_obj = exam_data.get("JSON", {})
            questions = json_obj.get("questions", [])
            all_questions.extend(questions)
    
    # Group questions by year and main question_id
    exams = {}  # {year: {question_id: [subquestions]}}
    
    for q in all_questions:
        year = q.get("year", "Unknown")
        q_id = q.get("question_id", "Q?")
        
        if year not in exams:
            exams[year] = {}
        if q_id not in exams[year]:
            exams[year][q_id] = []
        
        exams[year][q_id].append(q)
    
    print(f"Found {len(all_questions)} total questions")
    print(f"Across {len(exams)} exam years")
    print("=" * 60)
    
    # PDF filename patterns for each year
    pdf_patterns = {
        "2019": "AQA-8464C1H-QP-JUN19 (1).pdf",
        "2022": "AQA-8464C1H-QP-JUN22.PDF",
        "2023": "AQA-8464C1H-QP-JUN23.PDF",
        "2024": "AQA-8464C1H-QP-JUN24.PDF"
    }
    
    # Create output folder
    os.makedirs(output_folder, exist_ok=True)
    
    total_pdfs_created = 0
    
    # Process each year
    for year in sorted(exams.keys()):
        if year not in pdf_patterns:
            print(f"\nWarning: No PDF pattern defined for year {year}, skipping...")
            continue
        
        pdf_path = pdf_patterns[year]
        
        if not os.path.exists(pdf_path):
            print(f"\nWarning: PDF not found for {year}: {pdf_path}, skipping...")
            continue
        
        print(f"\n{'='*60}")
        print(f"Processing {year} exam: {pdf_path}")
        print(f"Found {len(exams[year])} main questions")
        print('-' * 60)
        
        # Open the PDF
        pdf_document = fitz.open(pdf_path)
        
        # Process each main question for this year
        for idx, (question_id, subquestions) in enumerate(sorted(exams[year].items()), 1):
            # Find page range for all subquestions
            # Try new field names first, fall back to old ones
            page_starts = [sq.get("question_page_start") or sq.get("page_start") for sq in subquestions if sq.get("question_page_start") or sq.get("page_start")]
            page_ends = [sq.get("question_page_end") or sq.get("page_end") for sq in subquestions if sq.get("question_page_end") or sq.get("page_end")]
            
            if not page_starts or not page_ends:
                print(f"  Warning: {question_id} missing page info, skipping")
                continue
            
            page_start = min(page_starts)
            page_end = max(page_ends)
            
            # Create new PDF for this question
            output_pdf = fitz.open()
            
            # Copy pages (0-indexed in fitz)
            for page_num in range(page_start - 1, page_end):
                if page_num < len(pdf_document):
                    output_pdf.insert_pdf(pdf_document, from_page=page_num, to_page=page_num)
            
            # Generate filename: YEAR_QXX.pdf
            safe_q_id = question_id.replace("/", "-").replace("\\", "-").replace(":", "-")
            filename = f"{year}_{safe_q_id}.pdf"
            filepath = os.path.join(output_folder, filename)
            
            # Save the PDF
            output_pdf.save(filepath)
            output_pdf.close()
            
            # Get subquestion IDs for display
            subq_ids = [sq.get("subquestion_id", "?") for sq in subquestions]
            page_range = f"p{page_start}" if page_start == page_end else f"p{page_start}-{page_end}"
            
            print(f"  {idx:2d}. {filename:20s} ({page_range:8s}) - {len(subquestions)} parts: {', '.join(subq_ids)}")
            
            total_pdfs_created += 1
        
        pdf_document.close()
    
    print("\n" + "=" * 60)
    print(f"Batch processing complete!")
    print(f"  Total question PDFs created: {total_pdfs_created}")
    print(f"  Saved to: {os.path.abspath(output_folder)}")
    
    return total_pdfs_created


if __name__ == "__main__":
    json_file = "array_of_jsons"
    
    if not os.path.exists(json_file):
        print(f"Error: File '{json_file}' not found!")
        exit(1)
    
    print(f"Reading exam data from: {json_file}")
    print("=" * 60)
    
    batch_split_pdfs_by_questions(json_file)
    
    print("\n" + "=" * 60)
    print("Done! All question PDFs are ready.")

