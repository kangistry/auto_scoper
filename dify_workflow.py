import cloudscraper
import json
import os
import glob
import csv

# Optional: PDF splitting (requires PyMuPDF)
try:
    import fitz  # PyMuPDF
    PDF_SPLITTING_AVAILABLE = True
except ImportError:
    PDF_SPLITTING_AVAILABLE = False
    print("Note: PyMuPDF not installed. PDF splitting will be skipped.")

# ============================================================
# CONFIGURATION
# ============================================================
API_KEY = "app-fd0HLVIwGDiY8QYvGdsbekQe"
BASE_URL = "https://dify.uplearn.co.uk/v1"

# Create a cloudscraper session to handle Cloudflare
scraper = cloudscraper.create_scraper(
    browser={
        'browser': 'chrome',
        'platform': 'windows',
        'desktop': True
    }
)

# Authentication Cookies
COOKIES = {
    # Google Cloud IAP Authentication
    "__Host-GCP_IAP_AUTH_TOKEN_220F89A4D796F67F": "AVBiXYjEI8cC8hszITkOkNRRLvd6mQZUd3t2cDBv9AG2u67js7Tk7wyXdc0-DxmI8AcE3xW-tddeOwqed--unVZ9-DbC1OIHrBPmGR2_JbUyuo2oufaoFjvP0yPcAsZoZidKmhGs-1FNWqs1A29m29lG3JGtITNd3vxLD9Wp0twRyF6r72UB-BEkBgYAePprjw1mBG5DmrxpyHhyM2hrWUYNIwRkAEPR2kfg4ONGPiAozB3gKoLGtpHKLghm9xC91udky9YKoIlgIw3eD5OWaUr_uSMAlK486kVHD9NUexqdo9I23TXIwgVoXWRg-I7_Fk5RnJIj-ssThfG4eCleIbN-kruUbYrwmdMnpSaePxSgRRpldRiP8sdFFbKoJrQ8WfDE8UPNb7vE-wTAVd1RLdo0VtpKpVOoyU7S1UVRSp_fXuMsuWXq_CPNaIwDdmI5O52uqYn9g-38jRMB3FB90tXSAa5i9yRaj6PG-G9KzxYM_fI2_-f5YbNAt2DrLbjY_HhkiC9IyOs6JMaqaeL4wwGmr1E61h7Y7hR2lMv3D4zJVE1wtWcxHJnb3-FA0Z02sljv485rPS9-OykQMA4jzlJeodmzs-jB2bjro8_tcyCR3nXXJfCR24Vnbw66n7jiai-iUCU91ksQ1u88robplU4yECO6NAeZq8c0r7uR-AtQi-M6eF92F7nlsH_dZmUaST8jSa6MevscxI6FO0SGIg7-iJ-lrl9g2bj_2LwGvM1WfNdPHDnv8ZOXHLapgW7Wa4buRmBf81hHbQUOl3nVUTZU280vZYGAnjmSlTogDDdPcAxoldY7s7f4DU3_rnTT54vWyrYFQWMXATGCTnw1hdG1KTr8dMfIOMJfhh7wVg_3rrP3dTll8l1zL9Wd6JPddU1Xn-VqD5N8DDP5q4P2Z-Nl3JV0Lph5IuqW93EFzVtD6OnTaYLwKNqyVUl6VaqGLBTqFqBXp6Ed8Iq1-154uHv6qmum6il4nQMHEAC13g3Z8Vx9sq1ngoizGtAYoHwKaUmSLuoCOk8id_H0LSzxTaPOd16v1xC0TYQ5ZeAMz-ptZQvSgD35dh-mS5a-ieVVGvZioskf1p9v-lgLM2illXgZ_iOrh_E3lwJTIu97I-tgP9u7J37pCkh2ZI-6MKn1WnJAIm3FhvcUr7nzAtZAVEcmkLCO604LUn4UHfp7ONNvvfK1tk_77K9FIw1Tz0w-FD92bjaaQ_8ehnLgyvOVIxJIGR-H2fboHIi9e9nVgr70ncecESD25EFUzQgu0gHmqqe2ZIAlk-9rlaRzEFNwfsJwJgbo8KG5XtiFvb0V7rbfenmdwkHjVXF9Kqd_P95cVq8RWL5iUN9_GHt0oc6nslvaiUcacrV1fLThPAmLwqsqe48xHebmc0rGm8pKC8seVXYKBqBbBVZxjM_VyWhFQaAYZU_BUdTQn73m59HUPyAuMusE2baElTXfM0FunHDP1zN4mgDgDU79ylnFcPQ6EGvrBdavuC3arvC2jwQ3pkUqKVEpR_RtiT7SqCh3crjLqPFhUypsk_NRwStUShjNCSoTJk5nAcKQR5ydaZD0ROjWgnHqwA_EUrM-FX0xSyX7QILxT2q9CKlL8pEX6Vb1rTKSg73vagn6uB7F88EhDP2JVQAbGl4aL4DMOmFVN47lPH-euN_T6OqLrKB0vkVss4Z8Z1taI_qk_J7JAEtq5esN1kZR0LBO5D4DAIUTTtqWC7h_BRjv8TArn5yWBQ",
    "GCP_IAP_UID": "109661608269002925364",
    # Uplearn auth token
    "auth-token": "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJ1cGxlYXJuLmNvLnVrIiwiZXhwIjoxNzcwMzk1MTkwLCJpYXQiOjE3Njc3MTY3OTAsImlzcyI6InVwbGVhcm4uY28udWsiLCJqdGkiOiJjYTk0NjkwMi01ODI2LTRiMGItYjkyYS1lNzFmZGIzZTYxZDEiLCJuYmYiOjE3Njc3MTY3ODksInN1YiI6IjQ0NTc4IiwidHlwIjoiYWNjZXNzIn0.yYCxKkqqPhRim7VDs1tBwi0T3HhCkWjraluGwP62RL5sZgXox5I0RGvUy5DFXQzJ5vFbLF2VGHFOaYkvrLYExQ",
    # Dify locale
    "locale": "en-US",
}

# ============================================================
# FILE UPLOAD FUNCTIONS
# ============================================================

def upload_file(file_path, user_id="default-user"):
    """
    Upload a single file to Dify.
    """
    url = f"{BASE_URL}/files/upload"
    
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }
    
    filename = os.path.basename(file_path)
    
    with open(file_path, 'rb') as f:
        files = {
            'file': (filename, f, get_mime_type(filename))
        }
        data = {
            'user': user_id
        }
        
        response = scraper.post(url, headers=headers, files=files, data=data, cookies=COOKIES)
    
    if response.status_code == 200 or response.status_code == 201:
        result = response.json()
        print(f"  Uploaded: {filename} -> ID: {result.get('id')}")
        return result
    else:
        print(f"  Error uploading {filename}: {response.status_code}")
        print(f"  {response.text}")
        return None


def upload_multiple_files(file_paths, user_id="default-user"):
    """
    Upload multiple files to Dify.
    """
    print(f"Uploading {len(file_paths)} files...")
    print("-" * 50)
    
    uploaded_files = []
    
    for file_path in file_paths:
        if not os.path.exists(file_path):
            print(f"  Warning: File not found: {file_path}")
            continue
        
        result = upload_file(file_path, user_id)
        if result:
            uploaded_files.append(result)
    
    print("-" * 50)
    print(f"Successfully uploaded: {len(uploaded_files)}/{len(file_paths)} files")
    
    return uploaded_files


def get_mime_type(filename):
    """Get MIME type based on file extension."""
    ext = filename.lower().split('.')[-1]
    mime_types = {
        'pdf': 'application/pdf',
        'json': 'application/json',
        'csv': 'text/csv',
        'txt': 'text/plain',
        'png': 'image/png',
        'jpg': 'image/jpeg',
        'jpeg': 'image/jpeg',
    }
    return mime_types.get(ext, 'application/octet-stream')


# ============================================================
# WORKFLOW FUNCTIONS
# ============================================================

def format_files_for_workflow(uploaded_files):
    """
    Format uploaded files for Dify workflow input.
    """
    return [
        {
            "type": "document",
            "transfer_method": "local_file",
            "upload_file_id": file_obj.get("id")
        }
        for file_obj in uploaded_files
    ]


def run_workflow(inputs, exam_papers_files=None, mark_scheme_files=None, 
                 user_id="default-user", response_mode="blocking"):
    """
    Run a Dify workflow with file inputs.
    """
    url = f"{BASE_URL}/workflows/run"
    
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    
    # Add file references directly to inputs
    if exam_papers_files:
        inputs["list_of_exam_papers"] = [
            {
                "type": "document",
                "transfer_method": "local_file",
                "upload_file_id": file_obj.get("id")
            }
            for file_obj in exam_papers_files
        ]
    
    if mark_scheme_files:
        inputs["list_of_mark_schemes"] = [
            {
                "type": "document",
                "transfer_method": "local_file",
                "upload_file_id": file_obj.get("id")
            }
            for file_obj in mark_scheme_files
        ]
    
    payload = {
        "inputs": inputs,
        "response_mode": response_mode,
        "user": user_id
    }
    
    print("\nRunning workflow...")
    print(f"  Inputs: {list(inputs.keys())}")
    print(f"  Exam papers: {len(exam_papers_files) if exam_papers_files else 0} files")
    print(f"  Mark schemes: {len(mark_scheme_files) if mark_scheme_files else 0} files")
    
    response = scraper.post(url, headers=headers, json=payload, cookies=COOKIES)
    
    if response.status_code == 200:
        result = response.json()
        print("\nWorkflow completed successfully!")
        return result
    else:
        print(f"\nError: {response.status_code}")
        print(response.text)
        return None


def run_workflow_streaming(inputs, exam_papers_files=None, mark_scheme_files=None, 
                           user_id="default-user"):
    """
    Run workflow with streaming response.
    """
    url = f"{BASE_URL}/workflows/run"
    
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    
    # Add file references directly to inputs
    if exam_papers_files:
        inputs["list_of_exam_papers"] = [
            {
                "type": "document",
                "transfer_method": "local_file",
                "upload_file_id": file_obj.get("id")
            }
            for file_obj in exam_papers_files
        ]
    
    if mark_scheme_files:
        inputs["list_of_mark_schemes"] = [
            {
                "type": "document",
                "transfer_method": "local_file",
                "upload_file_id": file_obj.get("id")
            }
            for file_obj in mark_scheme_files
        ]
    
    payload = {
        "inputs": inputs,
        "response_mode": "streaming",
        "user": user_id
    }
    
    print("\nRunning workflow (streaming)...")
    print(f"  Inputs: {list(inputs.keys())}")
    print(f"  Exam papers: {len(exam_papers_files) if exam_papers_files else 0} files")
    print(f"  Mark schemes: {len(mark_scheme_files) if mark_scheme_files else 0} files")
    print("-" * 50)
    
    response = scraper.post(url, headers=headers, json=payload, stream=True, cookies=COOKIES)
    
    full_response = ""
    final_result = None
    
    for line in response.iter_lines():
        if line:
            line_text = line.decode('utf-8')
            if line_text.startswith('data: '):
                try:
                    data = json.loads(line_text[6:])
                    event = data.get('event', '')
                    
                    if event == 'text_chunk':
                        chunk = data.get('data', {}).get('text', '')
                        print(chunk, end='', flush=True)
                        full_response += chunk
                    elif event == 'workflow_finished':
                        final_result = data
                    elif event == 'node_finished':
                        node_title = data.get('data', {}).get('title', '')
                        if node_title:
                            print(f"\n[Node completed: {node_title}]")
                except json.JSONDecodeError:
                    pass
    
    print("\n" + "-" * 50)
    print("Workflow finished!")
    
    return final_result if final_result else {"response": full_response}


# ============================================================
# MAIN EXECUTION
# ============================================================

def extract_session_from_filename(filename):
    """Extract session code (e.g., JUN24, NOV21) from filename."""
    import re
    match = re.search(r'(JUN|NOV)\d{2}', filename)
    return match.group(0) if match else None

def find_matching_ms(qp_file, ms_files):
    """Find the mark scheme that matches a question paper."""
    session = extract_session_from_filename(qp_file)
    if not session:
        return None
    
    for ms in ms_files:
        if session in ms:
            return ms
    return None

if __name__ == "__main__":
    # ============================================================
    # CONFIGURATION - Change these to process different papers
    # ============================================================
    PAPER_CODE = "C1F"  # e.g., "C1F", "C1H", "C2F", "C2H"
    PAPERS_FOLDER = "C1F Papers and Mark Schemes"  # Folder containing PDFs
    # ============================================================
    
    print("=" * 60)
    print(f"DIFY WORKFLOW - Exam Paper Indexer ({PAPER_CODE})")
    print("=" * 60)
    print(f"Source folder: {PAPERS_FOLDER}")
    
    # Build full path to papers folder
    papers_path = os.path.join(os.getcwd(), PAPERS_FOLDER)
    
    if not os.path.exists(papers_path):
        print(f"\nError: Folder not found: {papers_path}")
        exit(1)
    
    # Find ALL Question Papers (QP) in the folder
    all_files = os.listdir(papers_path)
    qp_files = [os.path.join(PAPERS_FOLDER, f) for f in all_files 
                if PAPER_CODE in f and 'QP' in f and f.upper().endswith('.PDF')]
    qp_files.sort()
    
    # Find ALL Mark Schemes (MS) in the folder
    ms_files = [os.path.join(PAPERS_FOLDER, f) for f in all_files 
                if PAPER_CODE in f and 'MS' in f and f.upper().endswith('.PDF')]
    ms_files.sort()
    
    print(f"\nFound {len(qp_files)} Question Papers (QP):")
    for f in qp_files:
        print(f"  - {os.path.basename(f)}")
    
    print(f"\nFound {len(ms_files)} Mark Schemes (MS):")
    for f in ms_files:
        print(f"  - {os.path.basename(f)}")
    
    if not qp_files:
        print(f"\nNo {PAPER_CODE} question papers found in {PAPERS_FOLDER}!")
        exit(1)
    
    # Match QPs with their corresponding MS
    paper_pairs = []
    for qp in qp_files:
        ms = find_matching_ms(qp, ms_files)
        session = extract_session_from_filename(qp)
        paper_pairs.append({
            'qp': qp,
            'ms': ms,
            'session': session
        })
    
    print(f"\n{'=' * 60}")
    print("PAPER PAIRS TO PROCESS:")
    print("=" * 60)
    for i, pair in enumerate(paper_pairs, 1):
        ms_status = os.path.basename(pair['ms']) if pair['ms'] else "NO MATCH FOUND"
        print(f"  {i}. {pair['session']}: {os.path.basename(pair['qp'])}")
        print(f"      -> {ms_status}")
    
    # Collect all results
    all_results = []
    successful = 0
    failed = 0
    
    # Process each paper pair ONE AT A TIME
    for i, pair in enumerate(paper_pairs, 1):
        print(f"\n{'=' * 60}")
        print(f"PROCESSING PAPER {i}/{len(paper_pairs)}: {pair['session']}")
        print("=" * 60)
        
        if not pair['ms']:
            print(f"  Skipping - no matching mark scheme found")
            failed += 1
            continue
        
        # Upload Question Paper
        print(f"\n  Uploading Question Paper: {pair['qp']}")
        uploaded_qp = upload_multiple_files([pair['qp']])
        
        # Upload Mark Scheme
        print(f"\n  Uploading Mark Scheme: {pair['ms']}")
        uploaded_ms = upload_multiple_files([pair['ms']])
        
        if not uploaded_qp or not uploaded_ms:
            print(f"  ERROR: Failed to upload files")
            failed += 1
            continue
        
        # Run the workflow for this pair
        workflow_inputs = {
            "Years_of_papers": pair['session']
        }
        
        result = run_workflow_streaming(
            inputs=workflow_inputs,
            exam_papers_files=uploaded_qp,
            mark_scheme_files=uploaded_ms,
            user_id="exam-indexer"
        )
        
        if result:
            all_results.append({
                'session': pair['session'],
                'qp_file': pair['qp'],
                'ms_file': pair['ms'],
                'result': result
            })
            successful += 1
            print(f"\n  [OK] Successfully processed {pair['session']}")
        else:
            failed += 1
            print(f"\n  [FAIL] Failed to process {pair['session']}")
        
        # Save intermediate results after each paper
        json_output = f"{PAPER_CODE}_workflow_results.json"
        with open(json_output, "w", encoding="utf-8") as f:
            json.dump(all_results, f, indent=2)
        print(f"  Progress saved to: {json_output}")
    
    # Final summary
    print(f"\n{'=' * 60}")
    print("BATCH PROCESSING COMPLETE")
    print("=" * 60)
    print(f"  Successful: {successful}/{len(paper_pairs)}")
    print(f"  Failed: {failed}/{len(paper_pairs)}")
    print(f"\n  All results saved to: {json_output}")
    
    # ============================================================
    # STEP 2: CONVERT JSON TO CSV
    # ============================================================
    if successful > 0:
        print(f"\n{'=' * 60}")
        print("CONVERTING JSON TO CSV")
        print("=" * 60)
        
        csv_output = f"{PAPER_CODE}_papers_combined.csv"
        all_questions = []
        
        for session_data in all_results:
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
                print(f"  [ERROR] {session}: Error extracting questions - {e}")
        
        # Write CSV
        fieldnames = [
            'session', 'exam', 'year', 'question_id', 'subquestion_id', 'type', 'marks',
            'AO', 'spec_reference', 'question_text', 'mark_scheme', 'extra_notes',
            'question_page_start', 'question_page_end', 'mark_scheme_start_page', 'mark_scheme_end_page',
            'has_figure', 'figure_labels_joined', 'has_table', 'table_labels_joined',
            'source_qp', 'source_ms'
        ]
        
        with open(csv_output, 'w', newline='', encoding='utf-8-sig') as csvfile:
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
        print(f"  CSV saved to: {csv_output}")
        
        # ============================================================
        # STEP 3: SPLIT PDFs BY QUESTION
        # ============================================================
        if PDF_SPLITTING_AVAILABLE:
            print(f"\n{'=' * 60}")
            print("SPLITTING PDFs BY QUESTION")
            print("=" * 60)
            
            # Group questions by session and question_id
            exams = {}
            session_to_pdf = {}
            
            for q in all_questions:
                session = q.get("session", "Unknown")
                q_id = q.get("question_id", "Q?")
                
                session_to_pdf[session] = q.get('source_qp', '')
                
                if session not in exams:
                    exams[session] = {}
                if q_id not in exams[session]:
                    exams[session][q_id] = []
                
                exams[session][q_id].append(q)
            
            # Extract exam code from first PDF filename for folder name
            # e.g., "AQA-8464C2H-QP-JUN18.PDF" -> "8464C2H"
            import re
            first_pdf = list(session_to_pdf.values())[0] if session_to_pdf else ""
            exam_code_match = re.search(r'(\d{4}[A-Z]\d?[A-Z]?)', first_pdf)
            exam_code = exam_code_match.group(1) if exam_code_match else "exam"
            
            output_folder = f"{exam_code}_question_pdfs"
            os.makedirs(output_folder, exist_ok=True)
            
            total_pdfs_created = 0
            
            for session in sorted(exams.keys()):
                pdf_path = session_to_pdf.get(session, '')
                
                if not pdf_path or not os.path.exists(pdf_path):
                    print(f"  [SKIP] {session}: PDF not found")
                    continue
                
                # Extract base name from PDF for file naming
                # e.g., "AQA-8464C2H-QP-JUN18.PDF" -> "AQA-8464C2H-JUN18"
                pdf_basename = os.path.splitext(os.path.basename(pdf_path))[0]
                # Remove "-QP" or "-W-MS" etc. to get cleaner name
                clean_basename = re.sub(r'-QP|-W?-?MS', '', pdf_basename)
                
                print(f"\n  Processing {session} -> {clean_basename}...")
                
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
                    
                    # Create filename: AQA-8464C2H-JUN18_Q01.pdf
                    q_id_str = f"Q{int(question_id):02d}" if isinstance(question_id, int) else f"Q{question_id}"
                    filename = f"{clean_basename}_{q_id_str}.pdf"
                    filepath = os.path.join(output_folder, filename)
                    
                    output_pdf.save(filepath)
                    output_pdf.close()
                    total_pdfs_created += 1
                
                pdf_document.close()
                print(f"    Created {len(exams[session])} question PDFs")
            
            print(f"\n  Total PDFs created: {total_pdfs_created}")
            print(f"  Saved to: {os.path.abspath(output_folder)}")
        else:
            print("\n  [SKIP] PDF splitting skipped (PyMuPDF not installed)")
    
    # ============================================================
    # FINAL SUMMARY
    # ============================================================
    print(f"\n{'=' * 60}")
    print("ALL TASKS COMPLETE!")
    print("=" * 60)
    print(f"  1. JSON results: workflow_results_all.json")
    print(f"  2. CSV export:   all_papers_combined.csv")
    if PDF_SPLITTING_AVAILABLE and successful > 0:
        print(f"  3. PDF splits:   {output_folder}/")
