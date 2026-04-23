import json
import csv

# Read the batch results JSON file
INPUT_FILE = 'workflow_results_all.json'
OUTPUT_FILE = 'all_papers_combined.csv'

try:
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
except json.JSONDecodeError:
    with open(INPUT_FILE, 'r', encoding='utf-8-sig') as f:
        data = json.load(f)

# Extract all questions from all sessions
all_questions = []

for session_data in data:
    session = session_data.get('session', 'Unknown')
    qp_file = session_data.get('qp_file', '')
    ms_file = session_data.get('ms_file', '')
    
    # Navigate the nested structure to get questions
    try:
        result = session_data.get('result', {})
        outputs = result.get('data', {}).get('outputs', {}).get('output', [])
        
        # output is a nested array: [[{JSON: {questions: [...]}}]]
        for output_group in outputs:
            for output_item in output_group:
                questions = output_item.get('JSON', {}).get('questions', [])
                
                for q in questions:
                    # Add session metadata to each question
                    q['session'] = session
                    q['source_qp'] = qp_file
                    q['source_ms'] = ms_file
                    all_questions.append(q)
                    
        print(f"  [OK] {session}: Extracted {len(questions)} questions")
        
    except Exception as e:
        print(f"  [ERROR] {session}: Error extracting questions - {e}")

print(f"\nTotal questions extracted: {len(all_questions)}")

# Define the field names (columns) - updated for new format
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

def clean_text_for_excel(text):
    """Clean text to be more Excel-friendly while preserving readability"""
    if not isinstance(text, str):
        return text
    return text

# Write to CSV file with proper quoting
with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8-sig') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
    
    # Write header
    writer.writeheader()
    
    # Write each question as a row
    for question in all_questions:
        row = {}
        for key in fieldnames:
            # Handle the joined fields specially
            if key == 'figure_labels_joined':
                value = question.get('figure_labels', [])
            elif key == 'table_labels_joined':
                value = question.get('table_labels', [])
            else:
                value = question.get(key, '')
            
            # Convert lists to comma-separated strings
            if isinstance(value, list):
                if len(value) > 0:
                    value = ', '.join(str(item) for item in value)
                else:
                    value = ''
            
            # Convert booleans to strings
            elif isinstance(value, bool):
                value = str(value).lower()
            
            # Clean text fields
            elif isinstance(value, str):
                value = clean_text_for_excel(value)
            
            row[key] = value
        
        writer.writerow(row)

print(f"\n{'=' * 50}")
print(f"Successfully converted {len(all_questions)} questions to {OUTPUT_FILE}")
print("=" * 50)
print("\nColumns included:")
for i, col in enumerate(fieldnames, 1):
    print(f"  {i:2}. {col}")
print(f"\nThe CSV file is ready to be opened in Excel!")

