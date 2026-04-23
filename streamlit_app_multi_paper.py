"""
Past Paper Processing Pipeline - Streamlit App
================================================
A complete workflow for processing AQA exam papers:
1. Upload question papers and mark schemes
2. Process through Dify API (parallelized)
3. Convert results to CSV
4. Split PDFs by question
5. Add Google Drive links
"""

import streamlit as st
import pandas as pd
import json
import os
import re
import io
import zipfile
import tempfile
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# PDF handling
try:
    import fitz  # PyMuPDF
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False

# API handling
try:
    import cloudscraper
    CLOUDSCRAPER_AVAILABLE = True
except ImportError:
    CLOUDSCRAPER_AVAILABLE = False

# =============================================================================
# CONFIGURATION
# =============================================================================
st.set_page_config(
    page_title="Past Paper Processor (Multi-Paper)",
    page_icon="📚",
    layout="wide"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .stProgress > div > div > div > div {
        background-color: #4CAF50;
    }
    .success-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
    }
    .info-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d1ecf1;
        border: 1px solid #bee5eb;
        color: #0c5460;
    }
</style>
""", unsafe_allow_html=True)

# =============================================================================
# API CONFIGURATION (stored in session state)
# =============================================================================
# Using single-paper workflow (better for individual paper processing)
DEFAULT_API_KEY = "app-hLnnq3Jn9etMuUsX90mQ0hHS"
DEFAULT_BASE_URL = "https://dify.uplearn.co.uk/v1"
CONFIG_FILE = "streamlit_config_multi_paper.json"

# Check if running in cloud (Streamlit Community Cloud)
def is_cloud_deployment():
    """Check if running on Streamlit Cloud."""
    return os.environ.get('STREAMLIT_SHARING_MODE') is not None or \
           hasattr(st, 'secrets') and len(st.secrets) > 0

def load_saved_config():
    """Load saved configuration - tries Streamlit Secrets first, then local file."""
    config = {}
    
    # Try Streamlit Secrets first (for cloud deployment)
    try:
        if hasattr(st, 'secrets'):
            if 'api_key' in st.secrets:
                config['api_key'] = st.secrets['api_key']
            if 'base_url' in st.secrets:
                config['base_url'] = st.secrets['base_url']
            if 'cookies' in st.secrets:
                config['cookies'] = dict(st.secrets['cookies'])
            if config:
                return config
    except Exception:
        pass
    
    # Fall back to local file (for local development)
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
    except Exception:
        pass
    return {}

def save_config(api_key, base_url, cookies):
    """Save configuration to file (only works locally, not on cloud)."""
    # Don't try to save files on cloud deployment
    if is_cloud_deployment():
        st.info("💡 Config saved to session only (cloud mode). Add to Streamlit Secrets for persistence.")
        return
    
    config = {
        'api_key': api_key,
        'base_url': base_url,
        'cookies': cookies
    }
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=2)
    except Exception as e:
        st.warning(f"Could not save config: {e}")

# =============================================================================
# RECOVERY / AUTO-SAVE FUNCTIONALITY
# =============================================================================
RECOVERY_FILE = "partial_results_recovery.json"

def save_partial_results(results, filename=RECOVERY_FILE):
    """Save partial results - to file locally, to session state on cloud."""
    try:
        # Filter to only successful results with actual data
        saveable_results = []
        for r in results:
            if r.get('success') and r.get('result'):
                saveable_results.append({
                    'identifier': r.get('identifier'),
                    'session': r.get('session'),
                    'paper_code': r.get('paper_code'),
                    'qp_file': r.get('qp_file'),
                    'ms_file': r.get('ms_file'),
                    'result': r.get('result'),
                    'success': True
                })
        
        if saveable_results:
            recovery_data = {
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'count': len(saveable_results),
                'results': saveable_results
            }
            
            # Always save to session state (works everywhere)
            st.session_state.recovery_data = recovery_data
            
            # Also try to save to file (works locally)
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(recovery_data, f, indent=2, ensure_ascii=False)
            except Exception:
                pass  # File save failed (probably cloud), but session state has it
            
            return len(saveable_results)
    except Exception as e:
        print(f"Error saving partial results: {e}")
    return 0

def load_partial_results(filename=RECOVERY_FILE):
    """Load partial results from file or session state."""
    # Try session state first
    if 'recovery_data' in st.session_state and st.session_state.recovery_data:
        data = st.session_state.recovery_data
        return data.get('results', []), data.get('timestamp', 'Unknown'), data.get('count', 0)
    
    # Try file (local mode)
    try:
        if os.path.exists(filename):
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('results', []), data.get('timestamp', 'Unknown'), data.get('count', 0)
    except Exception as e:
        print(f"Error loading partial results: {e}")
    return [], None, 0

def get_recovery_file_info():
    """Get info about recoverable results (from file or session state)."""
    # Check session state first
    if 'recovery_data' in st.session_state and st.session_state.recovery_data:
        data = st.session_state.recovery_data
        return {
            'exists': True,
            'timestamp': data.get('timestamp', 'Unknown'),
            'count': data.get('count', 0),
            'identifiers': [r.get('identifier') for r in data.get('results', [])],
            'source': 'session'
        }
    
    # Check file (local mode)
    try:
        if os.path.exists(RECOVERY_FILE):
            with open(RECOVERY_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return {
                    'exists': True,
                    'timestamp': data.get('timestamp', 'Unknown'),
                    'count': data.get('count', 0),
                    'identifiers': [r.get('identifier') for r in data.get('results', [])],
                    'source': 'file'
                }
    except Exception:
        pass
    return {'exists': False}

# Load saved config on startup
saved_config = load_saved_config()

if 'api_key' not in st.session_state:
    st.session_state.api_key = saved_config.get('api_key', DEFAULT_API_KEY)
if 'base_url' not in st.session_state:
    st.session_state.base_url = saved_config.get('base_url', DEFAULT_BASE_URL)
if 'cookies' not in st.session_state:
    st.session_state.cookies = saved_config.get('cookies', {})
if 'processing_results' not in st.session_state:
    st.session_state.processing_results = []
if 'csv_data' not in st.session_state:
    st.session_state.csv_data = None
if 'pdf_zip' not in st.session_state:
    st.session_state.pdf_zip = None

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_scraper():
    """Create a cloudscraper session."""
    if not CLOUDSCRAPER_AVAILABLE:
        return None
    return cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )

def upload_file_to_dify(file_bytes, filename, scraper, api_key, base_url, cookies, max_retries=3):
    """Upload a file to Dify and return the file info. Includes retry logic."""
    url = f"{base_url}/files/upload"
    headers = {"Authorization": f"Bearer {api_key}"}
    
    # Determine MIME type
    ext = filename.lower().split('.')[-1]
    mime_types = {'pdf': 'application/pdf', 'json': 'application/json'}
    mime_type = mime_types.get(ext, 'application/octet-stream')
    
    last_error = None
    for attempt in range(max_retries):
        try:
            files = {'file': (filename, io.BytesIO(file_bytes), mime_type)}
            data = {'user': 'streamlit-user'}
            
            response = scraper.post(url, headers=headers, files=files, data=data, cookies=cookies, timeout=60)
            
            if response.status_code in [200, 201]:
                # Check if response has content before parsing
                if not response.text or not response.text.strip():
                    raise Exception("Empty response from server - authentication may have failed")
                try:
                    return response.json()
                except json.JSONDecodeError as e:
                    # Show what we actually got
                    preview = response.text[:500] if response.text else "(empty)"
                    raise Exception(f"Invalid JSON response: {e}. Response was: {preview}")
            elif response.status_code in [401, 403]:
                raise Exception(f"{response.status_code} Auth Error - Cookies expired! Please refresh IAP and auth-token in sidebar.")
            elif response.status_code == 503:
                # Service unavailable - wait and retry
                time.sleep(2 ** attempt)
                continue
            else:
                last_error = f"Upload failed: {response.status_code} - {response.text[:200]}"
        except json.JSONDecodeError as e:
            last_error = f"JSON parse error: {e}"
        except Exception as e:
            last_error = str(e)
            if "Auth Error" in str(e) or "expired" in str(e).lower():
                raise  # Don't retry auth errors
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
    
    raise Exception(last_error or "Upload failed after retries")

def run_dify_workflow(qp_file_id, ms_file_id, session_name, scraper, api_key, base_url, cookies, max_retries=2, progress_callback=None):
    """Run the Dify workflow with STREAMING to avoid Cloudflare timeouts.
    
    Args:
        progress_callback: Optional function(node_status, elapsed_seconds) called on node events
    """
    url = f"{base_url}/workflows/run"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    # Single-paper workflow - only needs the two files
    inputs = {
        "Question_Paper": {
            "type": "document",
            "transfer_method": "local_file",
            "upload_file_id": qp_file_id
        },
        "Mark_Scheme": {
            "type": "document",
            "transfer_method": "local_file",
            "upload_file_id": ms_file_id
        }
    }
    
    payload = {
        "inputs": inputs,
        "response_mode": "streaming",  # Use streaming to avoid Cloudflare 524 timeouts
        "user": "streamlit-user"
    }
    
    last_error = None
    workflow_start_time = time.time()
    
    for attempt in range(max_retries):
        try:
            # Use streaming request
            response = scraper.post(url, headers=headers, json=payload, cookies=cookies, stream=True, timeout=600)
            
            if response.status_code in [401, 403]:
                raise Exception(f"{response.status_code} Auth Error - Cookies expired! Please refresh IAP and auth-token in sidebar.")
            elif response.status_code not in [200, 201]:
                # Try to get error details
                try:
                    error_text = response.text[:500] if response.text else "(empty)"
                except:
                    error_text = "(could not read response)"
                last_error = f"Workflow failed: {response.status_code} - {error_text}"
                if attempt < max_retries - 1:
                    time.sleep(5)
                    continue
                raise Exception(last_error)
            
            # Parse streaming response
            final_result = None
            events_received = 0
            last_event_type = None
            nodes_completed = []
            
            for line in response.iter_lines():
                if line:
                    line_text = line.decode('utf-8')
                    if line_text.startswith('data: '):
                        try:
                            data = json.loads(line_text[6:])
                            event = data.get('event', '')
                            events_received += 1
                            last_event_type = event
                            
                            if event == 'workflow_finished':
                                final_result = data
                                break
                            elif event == 'node_started':
                                node_title = data.get('data', {}).get('title', 'Unknown')
                                elapsed = time.time() - workflow_start_time
                                if progress_callback:
                                    progress_callback(f"▶ Starting: {node_title}", elapsed)
                            elif event == 'node_finished':
                                node_title = data.get('data', {}).get('title', 'Unknown')
                                elapsed = time.time() - workflow_start_time
                                nodes_completed.append(node_title)
                                if progress_callback:
                                    progress_callback(f"✓ Completed: {node_title}", elapsed)
                        except json.JSONDecodeError:
                            pass
            
            if final_result:
                final_result['_metadata'] = {
                    'total_time': time.time() - workflow_start_time,
                    'nodes_completed': nodes_completed,
                    'events_received': events_received
                }
                return final_result
            else:
                raise Exception(f"Workflow incomplete: received {events_received} events, last={last_event_type}")
                
        except Exception as e:
            last_error = str(e)
            if attempt < max_retries - 1 and ("timeout" in str(e).lower() or "524" in str(e)):
                time.sleep(5)
                continue
            raise
    
    raise Exception(last_error or "Workflow failed after retries")

def extract_session_from_filename(filename):
    """Extract session code (e.g., JUN24, NOV21) from filename."""
    match = re.search(r'(JUN|NOV|OCT)\d{2}', filename, re.IGNORECASE)
    return match.group(0).upper() if match else None

def extract_paper_identifier(filename):
    """
    Extract a unique paper identifier (paper_code + session) from filename.
    
    Examples:
    - 'AQA-84611H-QP-JUN22.PDF' -> '84611H-JUN22'
    - 'AQA-84612F-MS-JUN22.PDF' -> '84612F-JUN22'
    - 'AQA-8464C1H-QP-NOV20.PDF' -> '8464C1H-NOV20'
    
    This allows multiple papers from the same session to be distinguished.
    """
    if not filename:
        return None
    
    basename = os.path.basename(filename)
    
    # Try to extract paper code (e.g., 84611H, 84612F, 8464C1H)
    # Pattern: digits followed by optional letters/numbers (paper code)
    paper_match = re.search(r'(\d{4,5}[A-Z0-9]{1,4})', basename, re.IGNORECASE)
    paper_code = paper_match.group(1).upper() if paper_match else None
    
    # Extract session
    session_match = re.search(r'(JUN|NOV|OCT)\d{2}', basename, re.IGNORECASE)
    session = session_match.group(0).upper() if session_match else None
    
    if paper_code and session:
        return f"{paper_code}-{session}"
    elif session:
        # Fallback to just session if no paper code found
        return session
    else:
        return None

def extract_paper_code_from_identifier(identifier):
    """Extract just the paper code from an identifier like '84611H-JUN22'."""
    if not identifier or '-' not in identifier:
        return None
    return identifier.split('-')[0]

def extract_session_from_identifier(identifier):
    """Extract just the session from an identifier like '84611H-JUN22'."""
    if not identifier:
        return None
    match = re.search(r'(JUN|NOV|OCT)\d{2}', identifier, re.IGNORECASE)
    return match.group(0).upper() if match else None

def clean_source_filename(filename):
    """
    Clean a source filename for use as output base name.
    
    Examples:
    - 'AQA-8464C1H-QP-JUN22.PDF' -> 'AQA-8464C1H-JUN22'
    - 'Edexcel-1MA1-1F-QP-Jun22.pdf' -> 'Edexcel-1MA1-1F-Jun22'
    - 'C1F Papers/AQA-8464C1F-QP-NOV20.PDF' -> 'AQA-8464C1F-NOV20'
    """
    if not filename:
        return None
    
    # Get just the filename (no path)
    basename = os.path.basename(filename)
    
    # Remove extension
    name_without_ext = os.path.splitext(basename)[0]
    
    # Remove common suffixes: -QP, -MS, -W-MS, etc.
    cleaned = re.sub(r'[-_]?(QP|MS|W-MS|INS)(?=[-_]|$)', '', name_without_ext, flags=re.IGNORECASE)
    
    # Clean up any double dashes or trailing dashes
    cleaned = re.sub(r'--+', '-', cleaned)
    cleaned = cleaned.strip('-')
    
    return cleaned


def derive_folder_name(source_filename):
    """
    Derive a folder name from source filename.
    
    Examples:
    - 'AQA-8464C1H-QP-JUN22.PDF' -> '8464C1H_question_pdfs'
    - 'Edexcel-1MA1-1F-Jun22.pdf' -> '1MA1-1F_question_pdfs'
    """
    if not source_filename:
        return 'question_pdfs'
    
    basename = os.path.basename(source_filename)
    
    # Try to extract a meaningful code (AQA style, Edexcel style, etc.)
    # Look for patterns after 'AQA-', 'Edexcel-', 'OCR-', etc. or at start
    match = re.search(r'(?:AQA|Edexcel|OCR|WJEC|CCEA)?-?(\d{4}[A-Z0-9/-]+?)(?=-(?:QP|MS|JUN|NOV|OCT|JAN|MAR|MAY))', basename, re.IGNORECASE)
    if match:
        code = match.group(1).replace('/', '-').replace('\\', '-')
        return f"{code}_question_pdfs"
    
    # Fallback: use cleaned filename
    cleaned = clean_source_filename(source_filename)
    if cleaned:
        # Take first part before session
        parts = re.split(r'[-_](JUN|NOV|OCT|JAN|MAR|MAY)\d{2}', cleaned, flags=re.IGNORECASE)
        if parts[0]:
            return f"{parts[0]}_question_pdfs"
    
    return 'question_pdfs'

def convert_results_to_csv(results):
    """Convert workflow results to a pandas DataFrame.
    
    Handles both:
    - Multi-paper workflow: data.outputs.output = [[{JSON: {questions: [...]}}]]
    - Single-paper workflow: data.outputs.output = {questions: [...]} or similar
    """
    all_questions = []
    
    def parse_if_string(val):
        """Parse JSON string if needed, otherwise return as-is."""
        if isinstance(val, str):
            try:
                return json.loads(val)
            except json.JSONDecodeError:
                return val
        return val
    
    def find_questions(obj):
        """Recursively find questions array in the output structure."""
        obj = parse_if_string(obj)
        
        if not obj:
            return []
        
        # Direct questions array
        if isinstance(obj, dict):
            # Check for 'questions' key directly
            if 'questions' in obj:
                qs = parse_if_string(obj['questions'])
                if isinstance(qs, list):
                    return qs
            
            # Check for 'JSON' wrapper (multi-paper format)
            if 'JSON' in obj:
                return find_questions(obj['JSON'])
            
            # Check in 'output' key
            if 'output' in obj:
                return find_questions(obj['output'])
            
            # Check in 'outputs' key
            if 'outputs' in obj:
                return find_questions(obj['outputs'])
            
            # Check in 'data' key
            if 'data' in obj:
                return find_questions(obj['data'])
        
        # If it's a list, try each item
        if isinstance(obj, list):
            all_found = []
            for item in obj:
                found = find_questions(item)
                all_found.extend(found)
            return all_found
        
        return []
    
    for result_data in results:
        identifier = result_data.get('identifier', result_data.get('session', 'Unknown'))
        session = result_data.get('session', 'Unknown')
        paper_code = result_data.get('paper_code', '')
        qp_file = result_data.get('qp_file', '')
        ms_file = result_data.get('ms_file', '')
        
        try:
            result = result_data.get('result', {})
            result = parse_if_string(result)
            
            # Try to find questions using flexible search
            questions = find_questions(result)
            
            if not questions:
                st.warning(f"No questions found in {identifier} output. Structure: {type(result)}")
                continue
            
            for q in questions:
                if not isinstance(q, dict):
                    continue
                q['identifier'] = identifier  # Unique paper+session identifier
                q['session'] = session
                q['paper_code'] = paper_code
                q['source_qp'] = qp_file
                q['source_ms'] = ms_file
                
                # Create merge keys for easy joining with other CSVs
                q_id = q.get('question_id', '')
                subq_id = q.get('subquestion_id', '')
                
                # question_key: for merging at question level (e.g., "84611H-JUN22-Q1")
                if q_id:
                    q['question_key'] = f"{identifier}-Q{q_id}"
                else:
                    q['question_key'] = identifier
                
                # subquestion_key: for merging at subquestion level (e.g., "84611H-JUN22-1.1")
                if subq_id:
                    q['subquestion_key'] = f"{identifier}-{subq_id}"
                elif q_id:
                    q['subquestion_key'] = f"{identifier}-{q_id}"
                else:
                    q['subquestion_key'] = identifier
                
                # Convert lists to strings
                if 'figure_labels' in q:
                    labels = q.get('figure_labels', [])
                    if isinstance(labels, list):
                        q['figure_labels_joined'] = ', '.join(str(l) for l in labels)
                    else:
                        q['figure_labels_joined'] = str(labels) if labels else ''
                if 'table_labels' in q:
                    labels = q.get('table_labels', [])
                    if isinstance(labels, list):
                        q['table_labels_joined'] = ', '.join(str(l) for l in labels)
                    else:
                        q['table_labels_joined'] = str(labels) if labels else ''
                
                all_questions.append(q)
                
        except Exception as e:
            st.warning(f"Error extracting from {session}: {e}")
    
    if not all_questions:
        return None
    
    # Define column order - merge keys at the start for easy reference
    columns = [
        'subquestion_key', 'question_key', 'identifier', 'session', 'paper_code', 
        'exam', 'year', 'question_id', 'subquestion_id', 'type', 'marks',
        'AO', 'spec_reference', 'question_text', 'mark_scheme', 'extra_notes',
        'question_page_start', 'question_page_end', 'mark_scheme_start_page', 'mark_scheme_end_page',
        'has_figure', 'figure_labels_joined', 'has_table', 'table_labels_joined',
        'source_qp', 'source_ms'
    ]
    
    df = pd.DataFrame(all_questions)
    
    # Reorder columns (only include those that exist)
    existing_cols = [c for c in columns if c in df.columns]
    extra_cols = [c for c in df.columns if c not in columns]
    df = df[existing_cols + extra_cols]
    
    return df

def split_pdfs_by_question(df, qp_files_dict):
    """Split PDFs by question and return a zip file.
    
    Uses 'identifier' (paper_code + session) to uniquely identify each paper,
    allowing multiple papers from the same session to be processed correctly.
    """
    if not PDF_AVAILABLE:
        return None
    
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Determine which grouping key to use - prefer identifier if available
        if 'identifier' in df.columns and df['identifier'].notna().any():
            group_key = 'identifier'
        else:
            # Fallback to session for backward compatibility
            group_key = 'session'
        
        # Group questions by identifier/session and question_id
        grouped = df.groupby([group_key, 'question_id'])
        
        # Build a mapping of identifier/session -> cleaned source filename
        key_to_basename = {}
        if 'source_qp' in df.columns:
            for _, row in df[[group_key, 'source_qp']].drop_duplicates().iterrows():
                key = row[group_key]
                source_qp = row['source_qp'] if pd.notna(row['source_qp']) else ''
                cleaned = clean_source_filename(source_qp)
                if cleaned:
                    key_to_basename[key] = cleaned
        
        # Get folder name from first source file
        first_source = df['source_qp'].iloc[0] if 'source_qp' in df.columns else ''
        folder_name = derive_folder_name(first_source)
        
        output_folder = os.path.join(temp_dir, folder_name)
        os.makedirs(output_folder, exist_ok=True)
        
        pdfs_created = 0
        
        for (key, q_id), group in grouped:
            # Get the PDF file for this paper (using identifier or session)
            pdf_bytes = qp_files_dict.get(key)
            if not pdf_bytes:
                # Try fallback: if key is an identifier, try just the session part
                if group_key == 'identifier' and '-' in str(key):
                    session_part = extract_session_from_identifier(key)
                    pdf_bytes = qp_files_dict.get(session_part)
                if not pdf_bytes:
                    continue
            
            # Get page range
            page_starts = group['question_page_start'].dropna().astype(int).tolist()
            page_ends = group['question_page_end'].dropna().astype(int).tolist()
            
            if not page_starts or not page_ends:
                continue
            
            page_start = min(page_starts)
            page_end = max(page_ends)
            
            # Get base filename for this paper (from source_qp)
            base_name = key_to_basename.get(key, key)
            
            # Open PDF and extract pages
            try:
                pdf_doc = fitz.open(stream=pdf_bytes, filetype="pdf")
                output_pdf = fitz.open()
                
                for page_num in range(page_start - 1, page_end):
                    if page_num < len(pdf_doc):
                        output_pdf.insert_pdf(pdf_doc, from_page=page_num, to_page=page_num)
                
                # Save the question PDF: {cleaned_source_name}_Q01.pdf
                q_id_str = f"Q{int(q_id):02d}" if isinstance(q_id, (int, float)) else f"Q{q_id}"
                filename = f"{base_name}_{q_id_str}.pdf"
                filepath = os.path.join(output_folder, filename)
                
                output_pdf.save(filepath)
                output_pdf.close()
                pdf_doc.close()
                
                pdfs_created += 1
                
            except Exception as e:
                st.warning(f"Error splitting {key} Q{q_id}: {e}")
        
        # Create zip file
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for root, dirs, files in os.walk(output_folder):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, temp_dir)
                    zip_file.write(file_path, arcname)
        
        zip_buffer.seek(0)
        return zip_buffer.getvalue(), pdfs_created

def add_gdrive_links(df, mapping_df):
    """Add Google Drive links to the DataFrame based on the mapping."""
    # Create multiple lookup dicts for flexible matching
    link_by_filename = {}
    link_by_session_question = {}  # Key by (session, question_num) for flexible matching
    
    for _, row in mapping_df.iterrows():
        filename = row['filename']
        link = row['shareable_link']
        
        # Store by exact filename
        link_by_filename[filename] = link
        
        # Extract session and question number from filename for flexible matching
        # Handles: AQA-exam-NOV20_Q6.pdf, AQA-8464P1H-JUN22_Q01.pdf, etc.
        match = re.search(r'(JUN|NOV|OCT)\d{2}[^_]*_Q(\d+)', filename, re.IGNORECASE)
        if match:
            session = match.group(0).split('_')[0].upper()  # e.g., "NOV20"
            # Clean session (remove any suffix like "-CR")
            session = re.match(r'(JUN|NOV|OCT)\d{2}', session, re.IGNORECASE).group(0).upper()
            q_num = int(match.group(2))
            link_by_session_question[(session, q_num)] = link
    
    # Match links to questions
    links = []
    matched_count = 0
    
    for _, row in df.iterrows():
        session = str(row.get('session', '')).upper()
        q_id = row.get('question_id', '')
        
        link = None
        
        if pd.notna(q_id) and session:
            try:
                q_int = int(q_id) if isinstance(q_id, (int, float)) else int(str(q_id).replace('Q', '').split('.')[0])
                
                # Primary method: match by session + question number (most flexible)
                link = link_by_session_question.get((session, q_int))
                
            except (ValueError, TypeError):
                pass
        
        if link:
            matched_count += 1
        links.append(link)
    
    df['question_pdf_link'] = links
    return df

# =============================================================================
# MAIN APP
# =============================================================================

def check_password():
    """Simple password protection for the app."""
    # Check if password is configured in secrets
    try:
        if hasattr(st, 'secrets') and 'app_password' in st.secrets:
            if 'authenticated' not in st.session_state:
                st.session_state.authenticated = False
            
            if not st.session_state.authenticated:
                st.title("🔐 Auto Scoper")
                password = st.text_input("Enter password to access the app:", type="password")
                if password:
                    if password == st.secrets['app_password']:
                        st.session_state.authenticated = True
                        st.rerun()
                    else:
                        st.error("Incorrect password")
                return False
            return True
        else:
            # No password configured - allow access (for local development)
            return True
    except Exception:
        return True

def main():
    # Check password first
    if not check_password():
        st.stop()
    
    st.title("📚 Past Paper Processing Pipeline (Multi-Paper per Year)")
    st.markdown("**Supports multiple papers per year** (e.g., Paper 1H, 1F, 2H, 2F all from the same session). Papers are matched by full identifier (paper code + session).")
    
    # Sidebar for API configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        with st.expander("API Settings", expanded=False):
            st.session_state.api_key = st.text_input(
                "Dify API Key", 
                value=st.session_state.api_key,
                type="password"
            )
            st.session_state.base_url = st.text_input(
                "Dify Base URL",
                value=st.session_state.base_url
            )
        
        with st.expander("🍪 Authentication Cookies", expanded=False):
            st.markdown("""
            **To get fresh cookies:**
            1. Open Chrome DevTools (F12) on dify.uplearn.co.uk
            2. Go to Application → Cookies
            3. Copy the values below
            """)
            
            iap_token = st.text_input(
                "GCP IAP Token (__Host-GCP_IAP_AUTH_TOKEN_*)",
                value=st.session_state.cookies.get("__Host-GCP_IAP_AUTH_TOKEN_220F89A4D796F67F", ""),
                type="password"
            )
            
            auth_token = st.text_input(
                "Auth Token (auth-token)",
                value=st.session_state.cookies.get("auth-token", ""),
                type="password"
            )
            
            if iap_token or auth_token:
                st.session_state.cookies = {
                    "__Host-GCP_IAP_AUTH_TOKEN_220F89A4D796F67F": iap_token,
                    "GCP_IAP_UID": "109661608269002925364",
                    "auth-token": auth_token,
                    "locale": "en-US"
                }
            
            col1, col2 = st.columns(2)
            
            # Test connection button
            with col1:
                if st.button("🧪 Test"):
                    with st.spinner("Testing..."):
                        try:
                            scraper = get_scraper()
                            response = scraper.get(
                                f"{st.session_state.base_url}/parameters",
                                headers={"Authorization": f"Bearer {st.session_state.api_key}"},
                                cookies=st.session_state.cookies,
                                timeout=10
                            )
                            if response.status_code == 200:
                                try:
                                    data = response.json()
                                    st.success("✅ Connected!")
                                except:
                                    st.warning("⚠️ 200 but no JSON - partial auth issue")
                            elif response.status_code in [401, 403]:
                                st.error("❌ Cookies expired! Get fresh ones from Chrome DevTools")
                            else:
                                preview = response.text[:200] if response.text else "(empty)"
                                st.warning(f"⚠️ {response.status_code}: {preview}")
                        except Exception as e:
                            st.error(f"❌ {e}")
            
            # Save settings button
            with col2:
                if st.button("💾 Save"):
                    try:
                        save_config(
                            st.session_state.api_key,
                            st.session_state.base_url,
                            st.session_state.cookies
                        )
                        st.success("✅ Saved!")
                    except Exception as e:
                        st.error(f"❌ {e}")
        
        st.divider()
        
        # Status indicators
        st.subheader("📊 Status")
        st.write(f"API: {'✅ Configured' if st.session_state.api_key else '❌ Not set'}")
        st.write(f"Cloudscraper: {'✅ Available' if CLOUDSCRAPER_AVAILABLE else '❌ Not installed'}")
        st.write(f"PyMuPDF: {'✅ Available' if PDF_AVAILABLE else '❌ Not installed'}")
    
    # Main content - Tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "1️⃣ Upload & Process", 
        "2️⃣ View Results", 
        "3️⃣ Split PDFs",
        "4️⃣ Add Drive Links"
    ])
    
    # ==========================================================================
    # TAB 1: Upload & Process
    # ==========================================================================
    with tab1:
        st.header("Upload Papers & Process via Dify")
        
        # Check for recoverable results
        recovery_info = get_recovery_file_info()
        if recovery_info['exists']:
            with st.expander(f"🔄 **Recovery Available:** {recovery_info['count']} papers from {recovery_info['timestamp']}", expanded=True):
                st.markdown(f"**Papers recovered:** {', '.join(recovery_info['identifiers'])}")
                
                col_r1, col_r2, col_r3 = st.columns(3)
                with col_r1:
                    if st.button("📥 Load Recovered Results", type="primary"):
                        recovered_results, timestamp, count = load_partial_results()
                        if recovered_results:
                            st.session_state.processing_results = recovered_results
                            df = convert_results_to_csv(recovered_results)
                            st.session_state.csv_data = df
                            st.success(f"✅ Loaded {count} papers! Go to 'View Results' tab.")
                            st.rerun()
                
                with col_r2:
                    # Download recovery file directly
                    try:
                        with open(RECOVERY_FILE, 'r', encoding='utf-8') as f:
                            recovery_json = f.read()
                        st.download_button(
                            "💾 Download Recovery JSON",
                            recovery_json,
                            "partial_results_recovery.json",
                            "application/json"
                        )
                    except:
                        pass
                
                with col_r3:
                    if st.button("🗑️ Clear Recovery File"):
                        try:
                            os.remove(RECOVERY_FILE)
                            st.success("Cleared!")
                            st.rerun()
                        except:
                            pass
        
        st.divider()
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📄 Question Papers")
            qp_files = st.file_uploader(
                "Upload Question Paper PDFs",
                type=['pdf'],
                accept_multiple_files=True,
                key="qp_upload"
            )
        
        with col2:
            st.subheader("📝 Mark Schemes")
            ms_files = st.file_uploader(
                "Upload Mark Scheme PDFs",
                type=['pdf'],
                accept_multiple_files=True,
                key="ms_upload"
            )
        
        if qp_files and ms_files:
            st.divider()
            st.subheader("📋 Paper Matching")
            st.info("Papers are matched by paper code + session (e.g., 84611H-JUN22), allowing multiple papers per year.")
            
            # Auto-match papers by paper identifier (paper_code + session)
            matches = []
            qp_dict = {extract_paper_identifier(f.name): f for f in qp_files}
            ms_dict = {extract_paper_identifier(f.name): f for f in ms_files}
            
            # Get all unique identifiers
            all_identifiers = sorted(set(qp_dict.keys()) | set(ms_dict.keys()))
            
            for identifier in all_identifiers:
                if identifier:
                    matches.append({
                        'identifier': identifier,
                        'session': extract_session_from_identifier(identifier),
                        'paper_code': extract_paper_code_from_identifier(identifier),
                        'qp': qp_dict.get(identifier),
                        'ms': ms_dict.get(identifier)
                    })
            
            # Display matches
            for i, match in enumerate(matches):
                col1, col2, col3 = st.columns([1, 2, 2])
                with col1:
                    st.write(f"**{match['identifier']}**")
                with col2:
                    qp_status = f"✅ {match['qp'].name}" if match['qp'] else "❌ Missing"
                    st.write(f"QP: {qp_status}")
                with col3:
                    ms_status = f"✅ {match['ms'].name}" if match['ms'] else "❌ Missing"
                    st.write(f"MS: {ms_status}")
            
            # Process button
            valid_matches = [m for m in matches if m['qp'] and m['ms']]
            
            if valid_matches:
                st.divider()
                
                st.markdown("**Processing Options**")
                col_opt1, col_opt2 = st.columns(2)
                with col_opt1:
                    parallel = st.checkbox("⚡ Process in parallel", value=False, 
                                          help="Sequential is more reliable and avoids rate limits")
                    max_workers = st.slider("Max parallel workers", 1, 3, 2) if parallel else 1
                with col_opt2:
                    delay_between = st.slider("⏱️ Delay between papers (seconds)", 0, 60, 10,
                                             help="Add delay to avoid overwhelming the Dify server")
                
                if len(valid_matches) > 15:
                    st.warning(f"⚠️ Processing {len(valid_matches)} papers. Consider doing 10-15 at a time to avoid rate limits.")
                
                if st.button("🚀 Process Papers", type="primary"):
                    if not CLOUDSCRAPER_AVAILABLE:
                        st.error("cloudscraper is not installed. Run: pip install cloudscraper")
                        return
                    
                    results = []
                    qp_bytes_dict = {}
                    
                    # Create visible progress containers
                    st.divider()
                    status_container = st.container()
                    with status_container:
                        st.subheader("⏳ Processing...")
                        progress_bar = st.progress(0, text="Starting...")
                        status_text = st.empty()
                        log_area = st.expander("📋 Processing Log", expanded=True)
                    
                    # Create preview container for live CSV preview
                    st.divider()
                    preview_container = st.container()
                    with preview_container:
                        st.subheader("👀 Live Preview")
                        st.caption(f"💾 Results auto-saved to `{RECOVERY_FILE}` after each success. Safe to refresh!")
                        preview_stats = st.empty()
                        preview_table = st.empty()
                        preview_warning = st.empty()
                    
                    def update_preview(current_results):
                        """Update the live preview with current results."""
                        successful = [r for r in current_results if r.get('success')]
                        if not successful:
                            preview_stats.info("No successful results yet...")
                            return
                        
                        try:
                            preview_df = convert_results_to_csv(successful)
                            if preview_df is not None and len(preview_df) > 0:
                                # Show stats
                                papers_done = len(successful)
                                total_questions = len(preview_df)
                                questions_per_paper = preview_df.groupby('identifier').size().to_dict()
                                
                                stats_text = f"**{papers_done} papers processed** | **{total_questions} questions extracted**\n\n"
                                stats_text += "Questions per paper: " + ", ".join(
                                    f"{k}: {v}" for k, v in questions_per_paper.items()
                                )
                                preview_stats.markdown(stats_text)
                                
                                # Show sample of data (first few rows from each paper)
                                sample_df = preview_df.groupby('identifier').head(3).reset_index(drop=True)
                                display_cols = ['identifier', 'question_id', 'subquestion_id', 'type', 'marks', 'question_text']
                                display_cols = [c for c in display_cols if c in sample_df.columns]
                                
                                # Truncate question_text for display
                                if 'question_text' in sample_df.columns:
                                    sample_df = sample_df.copy()
                                    sample_df['question_text'] = sample_df['question_text'].astype(str).str[:100] + '...'
                                
                                preview_table.dataframe(sample_df[display_cols], use_container_width=True, height=200)
                                
                                # Check for potential issues (expect 20+ question parts per paper)
                                warnings = []
                                for paper, count in questions_per_paper.items():
                                    if count < 20:
                                        warnings.append(f"⚠️ {paper}: Only {count} question parts extracted (expected 20+)")
                                
                                if warnings:
                                    preview_warning.warning("\n".join(warnings))
                                else:
                                    preview_warning.empty()
                            else:
                                preview_stats.warning("No questions extracted from completed papers")
                        except Exception as e:
                            preview_stats.error(f"Preview error: {e}")
                    
                    # Capture session state values for thread safety
                    api_key = st.session_state.api_key
                    base_url = st.session_state.base_url
                    cookies = st.session_state.cookies.copy() if st.session_state.cookies else {}
                    
                    import datetime
                    
                    def process_single_paper(match, api_key=api_key, base_url=base_url, cookies=cookies, log_callback=None):
                        """Process a single paper pair."""
                        identifier = match['identifier']
                        session = match['session']  # Still used for Dify workflow
                        
                        def log(msg):
                            timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                            full_msg = f"[{timestamp}] {identifier}: {msg}"
                            if log_callback:
                                log_callback(full_msg)
                            return full_msg
                        
                        try:
                            # Create a new scraper for this thread
                            thread_scraper = get_scraper()
                            
                            # Read file bytes
                            log("Reading files...")
                            qp_bytes = match['qp'].read()
                            match['qp'].seek(0)  # Reset for potential reuse
                            ms_bytes = match['ms'].read()
                            match['ms'].seek(0)
                            
                            # Upload files
                            log("Uploading question paper...")
                            qp_result = upload_file_to_dify(
                                qp_bytes, match['qp'].name, thread_scraper,
                                api_key, base_url, cookies
                            )
                            log("Uploading mark scheme...")
                            ms_result = upload_file_to_dify(
                                ms_bytes, match['ms'].name, thread_scraper,
                                api_key, base_url, cookies
                            )
                            
                            # Run workflow
                            log("Running Dify workflow (this may take 1-5 minutes)...")
                            workflow_result = run_dify_workflow(
                                qp_result['id'], ms_result['id'], session,
                                thread_scraper, api_key, base_url, cookies
                            )
                            log("Workflow complete!")
                            
                            return {
                                'identifier': identifier,
                                'session': session,
                                'paper_code': match.get('paper_code'),
                                'qp_file': match['qp'].name,
                                'ms_file': match['ms'].name,
                                'result': workflow_result,
                                'qp_bytes': qp_bytes,
                                'success': True
                            }
                        except Exception as e:
                            log(f"Error: {e}")
                            return {
                                'identifier': identifier,
                                'session': session,
                                'error': str(e),
                                'success': False
                            }
                    
                    # Process papers
                    log_messages = []
                    
                    def add_log(msg):
                        log_messages.append(msg)
                        with log_area:
                            st.text("\n".join(log_messages[-10:]))  # Show last 10 messages
                    
                    add_log(f"Starting processing of {len(valid_matches)} papers...")
                    add_log(f"Parallel: {parallel}, Workers: {max_workers}")
                    
                    if parallel and max_workers > 1:
                        add_log("Using parallel processing...")
                        
                        # Track which papers are in progress
                        all_paper_ids = [m['identifier'] for m in valid_matches]
                        completed_ids = set()
                        failed_ids = set()
                        
                        # Create a status display for parallel progress
                        parallel_status = st.empty()
                        
                        def update_parallel_status():
                            in_progress = [p for p in all_paper_ids if p not in completed_ids and p not in failed_ids]
                            status_parts = []
                            if completed_ids:
                                status_parts.append(f"✅ Done: {', '.join(sorted(completed_ids))}")
                            if in_progress:
                                status_parts.append(f"🔄 In progress: {', '.join(in_progress)}")
                            if failed_ids:
                                status_parts.append(f"❌ Failed: {', '.join(sorted(failed_ids))}")
                            parallel_status.markdown(" | ".join(status_parts))
                        
                        update_parallel_status()  # Show initial "all in progress" state
                        
                        with ThreadPoolExecutor(max_workers=max_workers) as executor:
                            # Submit tasks with delays to avoid overwhelming the server
                            futures = {}
                            for idx, m in enumerate(valid_matches):
                                futures[executor.submit(process_single_paper, m)] = m
                                if delay_between > 0 and idx < len(valid_matches) - 1:
                                    add_log(f"⏳ Submitted {m['identifier']}, waiting {delay_between}s...")
                                    time.sleep(delay_between)
                            
                            completed = 0
                            
                            for future in as_completed(futures):
                                result = future.result()
                                results.append(result)
                                completed += 1
                                progress_bar.progress(completed / len(valid_matches), text=f"Processed {completed}/{len(valid_matches)}")
                                
                                if result['success']:
                                    completed_ids.add(result['identifier'])
                                    status_text.success(f"✅ Completed: {result['identifier']}")
                                    add_log(f"✅ {result['identifier']}: Success")
                                    qp_bytes_dict[result['identifier']] = result['qp_bytes']
                                    # Update live preview
                                    update_preview(results)
                                    # AUTO-SAVE: Save partial results after each success
                                    saved_count = save_partial_results(results)
                                    add_log(f"💾 Auto-saved {saved_count} papers to {RECOVERY_FILE}")
                                else:
                                    failed_ids.add(result['identifier'])
                                    status_text.error(f"❌ Failed: {result['identifier']}")
                                    add_log(f"❌ {result['identifier']}: {result.get('error', 'Unknown error')}")
                                
                                # Update parallel status display
                                update_parallel_status()
                    else:
                        add_log("Using sequential processing...")
                        for i, match in enumerate(valid_matches):
                            identifier = match['identifier']
                            session = match['session']
                            
                            progress_bar.progress(i / len(valid_matches), text=f"Processing {identifier}...")
                            add_log(f"Processing {identifier}...")
                            
                            # Use st.status for real-time node progress
                            with st.status(f"🔄 Processing {identifier}...", expanded=True) as status:
                                start_time = time.time()
                                node_progress = st.empty()  # For showing current node
                                timer_display = st.empty()  # For elapsed time
                                
                                try:
                                    thread_scraper = get_scraper()
                                    
                                    # Read files
                                    node_progress.write("📂 Reading files...")
                                    timer_display.write(f"⏱️ Elapsed: 0s")
                                    qp_bytes = match['qp'].read()
                                    match['qp'].seek(0)
                                    ms_bytes = match['ms'].read()
                                    match['ms'].seek(0)
                                    
                                    # Upload QP
                                    node_progress.write("📤 Uploading question paper...")
                                    timer_display.write(f"⏱️ Elapsed: {time.time() - start_time:.0f}s")
                                    qp_result = upload_file_to_dify(
                                        qp_bytes, match['qp'].name, thread_scraper,
                                        api_key, base_url, cookies
                                    )
                                    
                                    # Upload MS
                                    node_progress.write("📤 Uploading mark scheme...")
                                    timer_display.write(f"⏱️ Elapsed: {time.time() - start_time:.0f}s")
                                    ms_result = upload_file_to_dify(
                                        ms_bytes, match['ms'].name, thread_scraper,
                                        api_key, base_url, cookies
                                    )
                                    
                                    # Run workflow with progress callback
                                    node_progress.write("🔄 Starting Dify workflow...")
                                    
                                    def update_node_progress(node_status, elapsed):
                                        node_progress.write(f"🔄 {node_status}")
                                        timer_display.write(f"⏱️ Elapsed: {elapsed:.0f}s")
                                    
                                    workflow_result = run_dify_workflow(
                                        qp_result['id'], ms_result['id'], session,
                                        thread_scraper, api_key, base_url, cookies,
                                        progress_callback=update_node_progress
                                    )
                                    
                                    elapsed = time.time() - start_time
                                    
                                    # Show completion with node count
                                    metadata = workflow_result.get('_metadata', {})
                                    nodes = metadata.get('nodes_completed', [])
                                    node_progress.write(f"✅ Completed {len(nodes)} workflow nodes")
                                    
                                    result = {
                                        'identifier': identifier,
                                        'session': session,
                                        'paper_code': match.get('paper_code'),
                                        'qp_file': match['qp'].name,
                                        'ms_file': match['ms'].name,
                                        'result': workflow_result,
                                        'qp_bytes': qp_bytes,
                                        'success': True
                                    }
                                    status.update(label=f"✅ {identifier} completed in {elapsed:.0f}s", state="complete")
                                    
                                except Exception as e:
                                    elapsed = time.time() - start_time
                                    result = {
                                        'identifier': identifier,
                                        'session': session,
                                        'error': str(e),
                                        'success': False
                                    }
                                    node_progress.write(f"❌ Error: {e}")
                                    status.update(label=f"❌ {identifier} failed after {elapsed:.0f}s", state="error")
                            
                            results.append(result)
                            progress_bar.progress((i + 1) / len(valid_matches), text=f"Processed {i+1}/{len(valid_matches)}")
                            
                            if result['success']:
                                add_log(f"✅ {identifier}: Success ({elapsed:.0f}s)")
                                qp_bytes_dict[result['identifier']] = result['qp_bytes']
                                # Update live preview
                                update_preview(results)
                                # AUTO-SAVE: Save partial results after each success
                                saved_count = save_partial_results(results)
                                add_log(f"💾 Auto-saved {saved_count} papers to {RECOVERY_FILE}")
                            else:
                                add_log(f"❌ {identifier}: {result.get('error', 'Unknown')} ({elapsed:.0f}s)")
                            
                            # Add delay between papers to avoid rate limiting
                            if delay_between > 0 and i < len(valid_matches) - 1:
                                add_log(f"⏳ Waiting {delay_between}s before next paper...")
                                time.sleep(delay_between)
                    
                    # Store results
                    st.session_state.processing_results = results
                    st.session_state.qp_bytes_dict = qp_bytes_dict
                    
                    # Convert to CSV
                    successful_results = [r for r in results if r.get('success')]
                    if successful_results:
                        df = convert_results_to_csv(successful_results)
                        st.session_state.csv_data = df
                    
                    # Summary
                    st.divider()
                    success_count = sum(1 for r in results if r.get('success'))
                    st.success(f"✅ Processing complete: {success_count}/{len(valid_matches)} papers succeeded")
    
    # ==========================================================================
    # TAB 2: View Results
    # ==========================================================================
    with tab2:
        st.header("View & Download Results")
        
        # Option to load existing results
        with st.expander("📂 Load Existing Results (Skip API)", expanded=False):
            st.markdown("If you have existing workflow results JSON or CSV files, upload them here:")
            
            col1, col2 = st.columns(2)
            with col1:
                existing_json = st.file_uploader("Upload workflow_results.json", type=['json'], key="existing_json")
                if existing_json:
                    try:
                        json_data = json.load(existing_json)
                        if isinstance(json_data, list):
                            st.session_state.processing_results = json_data
                            df = convert_results_to_csv(json_data)
                            st.session_state.csv_data = df
                            st.success(f"✅ Loaded {len(json_data)} sessions from JSON")
                    except Exception as e:
                        st.error(f"Error loading JSON: {e}")
            
            with col2:
                existing_csv = st.file_uploader("Or upload existing CSV", type=['csv'], key="existing_csv")
                if existing_csv:
                    try:
                        df = pd.read_csv(existing_csv, encoding='utf-8-sig')
                        st.session_state.csv_data = df
                        st.success(f"✅ Loaded {len(df)} rows from CSV")
                    except Exception as e:
                        st.error(f"Error loading CSV: {e}")
        
        st.divider()
        
        if st.session_state.csv_data is not None:
            df = st.session_state.csv_data
            
            st.write(f"**Total questions:** {len(df)}")
            
            # Filters
            col1, col2 = st.columns(2)
            with col1:
                # Prefer identifier if available, fallback to session
                if 'identifier' in df.columns and df['identifier'].notna().any():
                    identifiers = ['All'] + sorted(df['identifier'].dropna().unique().tolist())
                    selected_paper = st.selectbox("Filter by paper", identifiers)
                    filter_col = 'identifier'
                else:
                    identifiers = ['All'] + sorted(df['session'].unique().tolist())
                    selected_paper = st.selectbox("Filter by session", identifiers)
                    filter_col = 'session'
            with col2:
                if 'type' in df.columns:
                    types = ['All'] + sorted(df['type'].dropna().unique().tolist())
                    selected_type = st.selectbox("Filter by type", types)
                else:
                    selected_type = 'All'
            
            # Apply filters
            filtered_df = df.copy()
            if selected_paper != 'All':
                filtered_df = filtered_df[filtered_df[filter_col] == selected_paper]
            if selected_type != 'All':
                filtered_df = filtered_df[filtered_df['type'] == selected_type]
            
            st.dataframe(filtered_df, use_container_width=True, height=400)
            
            # Download buttons
            st.divider()
            col1, col2 = st.columns(2)
            
            with col1:
                # Use bytes with UTF-8 BOM for proper Excel encoding
                csv_bytes = df.to_csv(index=False).encode('utf-8-sig')
                st.download_button(
                    "📥 Download CSV",
                    csv_bytes,
                    "papers_combined.csv",
                    "text/csv; charset=utf-8-sig"
                )
            
            with col2:
                # Excel download
                excel_buffer = io.BytesIO()
                df.to_excel(excel_buffer, index=False, engine='openpyxl')
                excel_buffer.seek(0)
                st.download_button(
                    "📥 Download Excel",
                    excel_buffer.getvalue(),
                    "papers_combined.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        else:
            st.info("No results yet. Process papers in the first tab to see results here.")
    
    # ==========================================================================
    # TAB 3: Split PDFs
    # ==========================================================================
    with tab3:
        st.header("Split PDFs by Question")
        
        if not PDF_AVAILABLE:
            st.error("PyMuPDF is not installed. Run: pip install PyMuPDF")
        elif st.session_state.csv_data is None:
            st.info("No CSV data available. Process papers first or upload an existing CSV.")
            
            uploaded_csv = st.file_uploader("Or upload existing CSV", type=['csv'])
            if uploaded_csv:
                st.session_state.csv_data = pd.read_csv(uploaded_csv)
                st.success("CSV loaded!")
        else:
            df = st.session_state.csv_data
            st.write(f"Ready to split {len(df)} questions")
            
            # Check if we have PDF bytes
            has_pdfs = hasattr(st.session_state, 'qp_bytes_dict') and st.session_state.qp_bytes_dict
            
            if not has_pdfs:
                st.warning("PDF files not in memory. Please upload them again:")
                qp_files_for_split = st.file_uploader(
                    "Upload Question Paper PDFs for splitting",
                    type=['pdf'],
                    accept_multiple_files=True,
                    key="qp_split_upload"
                )
                
                if qp_files_for_split:
                    qp_bytes_dict = {}
                    for f in qp_files_for_split:
                        # Use full identifier (paper_code + session) for unique keying
                        identifier = extract_paper_identifier(f.name)
                        if identifier:
                            qp_bytes_dict[identifier] = f.read()
                            f.seek(0)
                    st.session_state.qp_bytes_dict = qp_bytes_dict
                    has_pdfs = True
                    st.info(f"Loaded {len(qp_bytes_dict)} PDFs: {', '.join(qp_bytes_dict.keys())}")
            
            if has_pdfs:
                if st.button("✂️ Split PDFs", type="primary"):
                    with st.spinner("Splitting PDFs..."):
                        zip_data, pdf_count = split_pdfs_by_question(
                            df, st.session_state.qp_bytes_dict
                        )
                        st.session_state.pdf_zip = zip_data
                    
                    st.success(f"✅ Created {pdf_count} question PDFs!")
                
                if st.session_state.pdf_zip:
                    st.download_button(
                        "📥 Download Question PDFs (ZIP)",
                        st.session_state.pdf_zip,
                        "question_pdfs.zip",
                        "application/zip"
                    )
    
    # ==========================================================================
    # TAB 4: Add Google Drive Links
    # ==========================================================================
    with tab4:
        st.header("Add Google Drive Links")
        
        st.markdown("""
        After uploading your PDFs to Google Drive:
        1. Run the Google Apps Script to generate `pdf_links_mapping.csv`
        2. Upload that mapping file here
        3. Download your final spreadsheet with clickable links!
        """)
        
        mapping_file = st.file_uploader(
            "Upload pdf_links_mapping.csv from Google Drive",
            type=['csv'],
            key="mapping_upload"
        )
        
        if mapping_file and st.session_state.csv_data is not None:
            try:
                mapping_df = pd.read_csv(mapping_file)
                
                st.write(f"Loaded {len(mapping_df)} links from mapping file")
                
                if st.button("🔗 Add Links", type="primary"):
                    df_with_links = add_gdrive_links(st.session_state.csv_data.copy(), mapping_df)
                    
                    # Count matches
                    matched = df_with_links['question_pdf_link'].notna().sum()
                    st.success(f"✅ Matched {matched}/{len(df_with_links)} rows")
                    
                    # Preview
                    st.dataframe(
                        df_with_links[['session', 'question_id', 'subquestion_id', 'question_pdf_link']].head(20),
                        use_container_width=True
                    )
                    
                    # Download
                    st.divider()
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Use BytesIO with UTF-8 BOM for proper Excel encoding
                        csv_bytes = df_with_links.to_csv(index=False).encode('utf-8-sig')
                        st.download_button(
                            "📥 Download CSV with Links",
                            csv_bytes,
                            "papers_with_gdrive_links.csv",
                            "text/csv; charset=utf-8-sig"
                        )
                    
                    with col2:
                        excel_buffer = io.BytesIO()
                        df_with_links.to_excel(excel_buffer, index=False, engine='openpyxl')
                        excel_buffer.seek(0)
                        st.download_button(
                            "📥 Download Excel with Links",
                            excel_buffer.getvalue(),
                            "papers_with_gdrive_links.xlsx",
                            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    
            except Exception as e:
                st.error(f"Error loading mapping file: {e}")
        elif st.session_state.csv_data is None:
            st.info("Process papers first to have data to add links to.")

if __name__ == "__main__":
    main()
