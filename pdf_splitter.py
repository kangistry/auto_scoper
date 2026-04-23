import fitz  # PyMuPDF
import os
import json
from pathlib import Path

def split_pdf_by_questions(pdf_path, json_path, output_folder="question_pdfs"):
    """
    Split a PDF into separate PDFs for each main question (grouping all subquestions).
    
    Args:
        pdf_path: Path to the source PDF
        json_path: Path to the JSON with question metadata
        output_folder: Folder to save individual question PDFs
    
    Returns:
        Dictionary mapping questions to their PDF paths
    """
    # Load JSON
    with open(json_path, "r", encoding="utf-8") as f:
        json_data = json.load(f)
    
    questions = json_data.get("JSON", {}).get("questions", [])
    
    # Group subquestions by main question_id
    question_groups = {}
    for q in questions:
        q_id = q.get("question_id", "Q?")
        if q_id not in question_groups:
            question_groups[q_id] = []
        question_groups[q_id].append(q)
    
    # Create output folder
    os.makedirs(output_folder, exist_ok=True)
    
    # Open the source PDF
    pdf_document = fitz.open(pdf_path)
    pdf_name = Path(pdf_path).stem
    
    question_pdfs = []
    
    print(f"Processing: {pdf_path}")
    print(f"Grouping {len(questions)} subquestions into {len(question_groups)} main questions")
    print("=" * 60)
    
    for idx, (question_id, subquestions) in enumerate(sorted(question_groups.items()), 1):
        # Find the page range for all subquestions
        page_starts = [sq.get("page_start") for sq in subquestions if sq.get("page_start")]
        page_ends = [sq.get("page_end") for sq in subquestions if sq.get("page_end")]
        
        if not page_starts or not page_ends:
            print(f"Warning: Question {question_id} missing page info, skipping")
            continue
        
        page_start = min(page_starts)
        page_end = max(page_ends)
        
        # Create a new PDF for this main question
        output_pdf = fitz.open()
        
        # Copy pages from source PDF (page numbers are 0-indexed in fitz)
        for page_num in range(page_start - 1, page_end):
            if page_num < len(pdf_document):
                output_pdf.insert_pdf(pdf_document, from_page=page_num, to_page=page_num)
        
        # Generate filename
        safe_id = question_id.replace("/", "-").replace("\\", "-").replace(":", "-")
        filename = f"{pdf_name}_{safe_id}.pdf"
        filepath = os.path.join(output_folder, filename)
        
        # Save the question PDF
        output_pdf.save(filepath)
        output_pdf.close()
        
        # Store metadata
        subq_ids = [sq.get("subquestion_id") for sq in subquestions]
        pdf_info = {
            "question_id": question_id,
            "subquestion_ids": subq_ids,
            "subquestion_count": len(subquestions),
            "filename": filename,
            "path": filepath,
            "url": f"file:///{os.path.abspath(filepath)}",
            "page_start": page_start,
            "page_end": page_end,
            "page_count": page_end - page_start + 1
        }
        
        question_pdfs.append(pdf_info)
        
        page_range = f"p{page_start}" if page_start == page_end else f"p{page_start}-{page_end}"
        subq_list = ", ".join(subq_ids)
        print(f"  {idx}/{len(question_groups)} - {question_id} ({page_range})")
        print(f"      Contains: {subq_list}")
        print(f"      -> {filename}")
    
    pdf_document.close()
    
    print("\n" + "=" * 60)
    print(f"Split complete! Created {len(question_pdfs)} question PDFs")
    print(f"PDFs saved to: {os.path.abspath(output_folder)}")
    
    # Save metadata
    metadata_path = os.path.join(output_folder, f"{pdf_name}_split_metadata.json")
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(question_pdfs, f, indent=2)
    
    print(f"Metadata saved to: {metadata_path}")
    
    return question_pdfs


def create_csv_with_pdf_links(json_path, pdf_metadata_path, output_csv="past_paper_with_pdfs.csv"):
    """
    Create CSV from JSON with links to question PDFs (one PDF per main question).
    """
    import csv
    
    # Load JSON
    with open(json_path, "r", encoding="utf-8") as f:
        json_data = json.load(f)
    
    questions = json_data.get("JSON", {}).get("questions", [])
    
    # Load PDF metadata
    with open(pdf_metadata_path, "r", encoding="utf-8") as f:
        pdf_metadata = json.load(f)
    
    # Create a mapping from question_id to PDF info
    # (since we now have one PDF per main question, not per subquestion)
    pdf_map = {item["question_id"]: item for item in pdf_metadata}
    
    # Define fieldnames
    fieldnames = [
        'exam',
        'question_id',
        'subquestion_id',
        'type',
        'marks',
        'AO',
        'spec_reference',
        'question_text',
        'mark_scheme',
        'extra_notes',
        'page_start',
        'page_end',
        'has_figure',
        'figure_labels_joined',
        'has_table',
        'table_labels_joined',
        'question_pdf_filename',
        'question_pdf_path',
        'question_pdf_url'
    ]
    
    # Write CSV
    with open(output_csv, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        
        for question in questions:
            row = {}
            
            # Copy existing fields
            for key in fieldnames[:16]:  # All fields before the PDF ones
                value = question.get(key, '')
                
                # Convert lists to strings
                if isinstance(value, list):
                    value = ', '.join(str(item) for item in value) if value else ''
                
                row[key] = value
            
            # Add PDF information based on main question_id
            q_id = question.get('question_id', '')
            if q_id in pdf_map:
                pdf_info = pdf_map[q_id]
                row['question_pdf_filename'] = pdf_info['filename']
                row['question_pdf_path'] = pdf_info['path']
                row['question_pdf_url'] = pdf_info['url']
            else:
                row['question_pdf_filename'] = ''
                row['question_pdf_path'] = ''
                row['question_pdf_url'] = ''
            
            writer.writerow(row)
    
    print(f"\nCSV with PDF links saved to: {output_csv}")
    return output_csv


if __name__ == "__main__":
    pdf_file = "AQA-8464C1H-QP-JUN19 (1).pdf"
    json_file = "past_paper.json"
    
    if not os.path.exists(pdf_file) or not os.path.exists(json_file):
        print(f"Error: Required files not found!")
        print(f"  PDF: {pdf_file} - {'Found' if os.path.exists(pdf_file) else 'NOT FOUND'}")
        print(f"  JSON: {json_file} - {'Found' if os.path.exists(json_file) else 'NOT FOUND'}")
        exit(1)
    
    # Step 1: Split PDF by questions
    pdf_metadata = split_pdf_by_questions(pdf_file, json_file)
    
    # Step 2: Create CSV with PDF links
    pdf_name = Path(pdf_file).stem
    metadata_path = os.path.join("question_pdfs", f"{pdf_name}_split_metadata.json")
    
    print("\n" + "=" * 60)
    print("Creating CSV with PDF links...")
    create_csv_with_pdf_links(json_file, metadata_path)
    
    print("\n" + "=" * 60)
    print("All done!")
    print(f"  - {len(pdf_metadata)} question PDFs created")
    print(f"  - CSV file created with links to each question PDF")

