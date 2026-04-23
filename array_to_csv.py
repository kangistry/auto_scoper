import json
import csv
import os

def convert_array_of_jsons_to_csv(json_path, output_csv="all_exams.csv"):
    """
    Convert an array of exam JSONs into one big CSV.
    Automatically detects all unique field names across all questions.
    """
    # Load the JSON
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    # Navigate the structure: output -> array of arrays -> array of objects with "JSON" -> "questions"
    all_questions = []
    
    # Handle the nested structure
    output_array = data.get("output", [])
    
    for exam_group in output_array:
        # Each exam_group is an array
        for exam_data in exam_group:
            # Each exam_data has a "JSON" key with "questions"
            json_obj = exam_data.get("JSON", {})
            questions = json_obj.get("questions", [])
            all_questions.extend(questions)
    
    print(f"Found {len(all_questions)} total questions across all exams")
    
    if not all_questions:
        print("Error: No questions found in the JSON!")
        return
    
    # Automatically determine all unique field names
    all_fieldnames = set()
    for q in all_questions:
        all_fieldnames.update(q.keys())
    
    # Sort fieldnames for consistent ordering, but prioritize common ones first
    priority_fields = [
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
        'page_start',
        'page_end',
        'has_figure',
        'figure_labels_joined',
        'has_table',
        'table_labels_joined'
    ]
    
    # Start with priority fields that exist, then add any others
    fieldnames = [f for f in priority_fields if f in all_fieldnames]
    remaining_fields = sorted(all_fieldnames - set(fieldnames))
    fieldnames.extend(remaining_fields)
    
    print(f"\nCSV will have {len(fieldnames)} columns:")
    print(f"  {', '.join(fieldnames)}")
    
    # Count questions by exam/year
    exam_counts = {}
    for q in all_questions:
        year = q.get('year', 'Unknown')
        exam_counts[year] = exam_counts.get(year, 0) + 1
    
    print(f"\nQuestions by year:")
    for year, count in sorted(exam_counts.items()):
        print(f"  {year}: {count} questions")
    
    # Write to CSV
    with open(output_csv, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        
        for question in all_questions:
            row = {}
            
            for key in fieldnames:
                value = question.get(key, '')
                
                # Convert lists to comma-separated strings
                if isinstance(value, list):
                    value = ', '.join(str(item) for item in value) if value else ''
                
                row[key] = value
            
            writer.writerow(row)
    
    print(f"\nCSV saved to: {output_csv}")
    print(f"Total rows: {len(all_questions)}")
    return output_csv


if __name__ == "__main__":
    json_file = "array_of_jsons"
    
    if not os.path.exists(json_file):
        print(f"Error: File '{json_file}' not found!")
        exit(1)
    
    print(f"Processing: {json_file}")
    print("=" * 60)
    
    convert_array_of_jsons_to_csv(json_file)
    
    print("\n" + "=" * 60)
    print("Done! You can now open all_exams.csv in Excel")


