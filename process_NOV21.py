import json
import csv
import os
import fitz  # PyMuPDF

def process_nov21_json(json_path="C1H_NOV_21_result.json"):
    """
    Process the NOV21 result JSON:
    1. Extract questions to CSV (matching format of C1H_papers_combined.csv)
    2. Split the PDF into question PDFs
    """
    
    # Load the JSON
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    # Extract questions from the nested structure
    all_questions = []
    output_array = data.get("output", [])
    
    for exam_group in output_array:
        for exam_data in exam_group:
            json_obj = exam_data.get("JSON", {})
            questions = json_obj.get("questions", [])
            all_questions.extend(questions)
    
    print(f"Found {len(all_questions)} questions in NOV21 paper")
    
    return all_questions


def create_csv(questions, output_csv="C1H_NOV21.csv"):
    """
    Create a CSV file matching the format of C1H_papers_combined.csv
    """
    # Define field order to match existing CSV
    fieldnames = [
        'session',
        'exam',
        'year',
        'question_id',
        'subquestion_id',
        'type',
        'marks',
        'AO',
        'spec_reference',
        'question_text',
        'mark_scheme',
        'extra_notes',
        'question_page_start',
        'question_page_end',
        'mark_scheme_start_page',
        'mark_scheme_end_page',
        'has_figure',
        'figure_labels_joined',
        'has_table',
        'table_labels_joined',
        'source_qp',
        'source_ms'
    ]
    
    # Source PDF paths for NOV21
    source_qp = r"C1H Papers and Mark Schemes\AQA-8464C1H-QP-NOV21.PDF"
    source_ms = r"C1H Papers and Mark Schemes\AQA-8464C1H-MS-NOV21.PDF"
    
    with open(output_csv, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        
        for q in questions:
            # Join list fields
            figure_labels = q.get('figure_labels', [])
            table_labels = q.get('table_labels', [])
            
            row = {
                'session': 'NOV21',
                'exam': q.get('exam', ''),
                'year': q.get('year', '2021'),
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
                'figure_labels_joined': '; '.join(figure_labels) if figure_labels else '',
                'has_table': q.get('has_table', ''),
                'table_labels_joined': '; '.join(table_labels) if table_labels else '',
                'source_qp': source_qp,
                'source_ms': source_ms
            }
            
            writer.writerow(row)
    
    print(f"CSV saved to: {output_csv}")
    print(f"  Total rows: {len(questions)}")
    return output_csv


def create_question_pdfs(questions, output_folder="8464C1H_question_pdfs"):
    """
    Split the NOV21 PDF into individual question PDFs
    """
    pdf_path = r"C1H Papers and Mark Schemes\AQA-8464C1H-QP-NOV21.PDF"
    
    if not os.path.exists(pdf_path):
        print(f"Error: PDF not found: {pdf_path}")
        return 0
    
    # Group questions by main question_id
    grouped = {}
    for q in questions:
        q_id = q.get('question_id', 'Q?')
        if q_id not in grouped:
            grouped[q_id] = []
        grouped[q_id].append(q)
    
    print(f"\nCreating question PDFs...")
    print(f"  Source: {pdf_path}")
    print(f"  Output folder: {output_folder}")
    print(f"  Main questions: {len(grouped)}")
    print("-" * 60)
    
    os.makedirs(output_folder, exist_ok=True)
    
    pdf_document = fitz.open(pdf_path)
    pdfs_created = 0
    
    for idx, (question_id, subquestions) in enumerate(sorted(grouped.items()), 1):
        # Find page range
        page_starts = [sq.get('question_page_start') for sq in subquestions if sq.get('question_page_start')]
        page_ends = [sq.get('question_page_end') for sq in subquestions if sq.get('question_page_end')]
        
        if not page_starts or not page_ends:
            print(f"  Warning: Q{question_id} missing page info, skipping")
            continue
        
        page_start = min(page_starts)
        page_end = max(page_ends)
        
        # Create new PDF
        output_pdf = fitz.open()
        
        # Copy pages (0-indexed in fitz)
        for page_num in range(page_start - 1, page_end):
            if page_num < len(pdf_document):
                output_pdf.insert_pdf(pdf_document, from_page=page_num, to_page=page_num)
        
        # Filename: NOV21_Q01.pdf format
        filename = f"NOV21_Q{str(question_id).zfill(2)}.pdf"
        filepath = os.path.join(output_folder, filename)
        
        output_pdf.save(filepath)
        output_pdf.close()
        
        # Get subquestion IDs for display
        subq_ids = [sq.get('subquestion_id', '?') for sq in subquestions]
        page_range = f"p{page_start}" if page_start == page_end else f"p{page_start}-{page_end}"
        
        print(f"  {idx:2d}. {filename:20s} ({page_range:8s}) - {len(subquestions)} parts: {', '.join(subq_ids)}")
        pdfs_created += 1
    
    pdf_document.close()
    
    print("-" * 60)
    print(f"Created {pdfs_created} question PDFs")
    
    return pdfs_created


if __name__ == "__main__":
    print("=" * 60)
    print("Processing C1H NOV21 Paper")
    print("=" * 60)
    
    # Step 1: Load questions from JSON
    questions = process_nov21_json()
    
    # Step 2: Create CSV
    print("\n" + "=" * 60)
    print("Step 1: Creating CSV")
    print("=" * 60)
    create_csv(questions)
    
    # Step 3: Create question PDFs
    print("\n" + "=" * 60)
    print("Step 2: Creating Question PDFs")
    print("=" * 60)
    create_question_pdfs(questions)
    
    print("\n" + "=" * 60)
    print("Done!")
    print("  - CSV: C1H_NOV21.csv")
    print("  - Question PDFs: 8464C1H_question_pdfs/NOV21_Q*.pdf")
    print("=" * 60)
