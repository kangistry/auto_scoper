import json
import csv

# Read the JSON file
try:
    with open('past_paper.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
except json.JSONDecodeError:
    # Try with utf-8-sig to handle BOM
    with open('past_paper.json', 'r', encoding='utf-8-sig') as f:
        data = json.load(f)

# Get the questions array
questions = data['JSON']['questions']

# Define the field names (columns)
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
    'table_labels_joined'
]

def clean_text_for_excel(text):
    """Clean text to be more Excel-friendly while preserving readability"""
    if not isinstance(text, str):
        return text
    
    # Option 1: Keep \n as-is (Excel will show them as line breaks in cells)
    # Just return the text as-is - CSV module will handle quoting
    return text
    
    # Option 2: Replace \n with a space (uncomment to use)
    # return text.replace('\n', ' ')
    
    # Option 3: Replace \n with " | " for visual separation (uncomment to use)
    # return text.replace('\n', ' | ')
    
    # Option 4: Replace \n with actual line breaks Excel understands (uncomment to use)
    # Note: This keeps line breaks but they display properly in Excel
    # return text  # Already works! CSV module handles it

# Write to CSV file with proper quoting
with open('past_paper.csv', 'w', newline='', encoding='utf-8-sig') as csvfile:
    # QUOTING_ALL ensures every field is quoted, preventing any delimiter issues
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
    
    # Write header
    writer.writeheader()
    
    # Write each question as a row
    for question in questions:
        # Create a copy and process each field
        row = {}
        for key in fieldnames:
            value = question.get(key, '')
            
            # Convert lists to comma-separated strings
            if isinstance(value, list):
                if len(value) > 0:
                    value = ', '.join(str(item) for item in value)
                else:
                    value = ''
            
            # Clean text fields
            elif isinstance(value, str):
                value = clean_text_for_excel(value)
            
            row[key] = value
        
        writer.writerow(row)

print(f"Successfully converted {len(questions)} questions to past_paper.csv")
print("\nHandled potential issues:")
print("  - Newlines (\\n) - Quoted properly for Excel")
print("  - Commas - Fields quoted to prevent delimiter issues")
print("  - Special characters - Preserved with UTF-8-BOM encoding")
print("  - Lists (figure_labels, table_labels) - Converted to comma-separated text")
print("\nThe CSV file is ready to be opened in Excel!")
print("\nTip: In Excel, double-click the row height divider to auto-fit cell heights")
print("     for better viewing of multi-line content.")

