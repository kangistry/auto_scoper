import fitz  # PyMuPDF
import os
import json
from pathlib import Path

def extract_tables_and_images_from_pdf(pdf_path, output_folder="extracted_images",
                                       min_width=200, min_height=200, min_area=40000):
    """
    Extract all images AND table regions from a PDF file.
    
    This function:
    1. Extracts embedded images (photos, diagrams)
    2. Detects and captures table regions as images
    3. Filters out small/irrelevant images
    
    Args:
        pdf_path: Path to the PDF file
        output_folder: Folder to save extracted images
        min_width: Minimum width in pixels
        min_height: Minimum height in pixels
        min_area: Minimum area (width * height)
    
    Returns:
        Dictionary with image and table information
    """
    # Create output folder
    os.makedirs(output_folder, exist_ok=True)
    
    pdf_name = Path(pdf_path).stem
    pdf_document = fitz.open(pdf_path)
    
    all_data = []
    image_counter = 0
    table_counter = 0
    filtered_count = 0
    
    print(f"Processing: {pdf_path}")
    print(f"Total pages: {len(pdf_document)}")
    print("-" * 60)
    
    # Iterate through each page
    for page_num in range(len(pdf_document)):
        page = pdf_document[page_num]
        
        # STEP 1: Extract embedded images
        image_list = page.get_images(full=True)
        
        if image_list:
            print(f"\nPage {page_num + 1}: Found {len(image_list)} embedded image(s)")
        
        for img_index, img in enumerate(image_list):
            xref = img[0]
            base_image = pdf_document.extract_image(xref)
            image_bytes = base_image["image"]
            image_ext = base_image["ext"]
            width = base_image["width"]
            height = base_image["height"]
            area = width * height
            
            # Filter out small images
            if width < min_width or height < min_height or area < min_area:
                print(f"  - Skipped image: {width}x{height} (too small)")
                filtered_count += 1
                continue
            
            # Save the image
            image_filename = f"{pdf_name}_page{page_num + 1}_img{image_counter + 1}.{image_ext}"
            image_path = os.path.join(output_folder, image_filename)
            
            with open(image_path, "wb") as image_file:
                image_file.write(image_bytes)
            
            # Store metadata
            image_info = {
                "type": "embedded_image",
                "id": image_counter,
                "filename": image_filename,
                "path": image_path,
                "url": f"file:///{os.path.abspath(image_path)}",
                "page": page_num + 1,
                "format": image_ext,
                "size_bytes": len(image_bytes),
                "width": width,
                "height": height
            }
            
            all_data.append(image_info)
            image_counter += 1
            
            print(f"  + Extracted image: {image_filename} ({width}x{height})")
        
        # STEP 2: Detect and extract table regions
        text = page.get_text()
        text_lower = text.lower()
        
        # Table detection: look for "Table" keyword OR table-like patterns
        has_table_keyword = "table" in text_lower
        has_table_structure = any(x in text_lower for x in ["| ", "|---", "row", "column"])
        
        if has_table_keyword or has_table_structure:
            print(f"  > Detected table text on page {page_num + 1}")
            
            # Get all text blocks with their positions
            blocks = page.get_text("dict")["blocks"]
            
            # Look for table structures (lines, rectangles)
            drawings = page.get_drawings()
            
            # If we find horizontal/vertical lines (typical of tables), capture that region
            if drawings:
                # Find bounding box of all drawings (likely table borders)
                x0 = min(d["rect"].x0 for d in drawings if d.get("rect"))
                y0 = min(d["rect"].y0 for d in drawings if d.get("rect"))
                x1 = max(d["rect"].x1 for d in drawings if d.get("rect"))
                y1 = max(d["rect"].y1 for d in drawings if d.get("rect"))
                
                table_rect = fitz.Rect(x0, y0, x1, y1)
                
                # Expand slightly to capture all content
                table_rect = table_rect + (-5, -5, 5, 5)
                
                # Render this region as an image
                mat = fitz.Matrix(2, 2)  # 2x zoom for better quality
                pix = page.get_pixmap(matrix=mat, clip=table_rect)
                
                # Save table as image
                table_filename = f"{pdf_name}_page{page_num + 1}_table{table_counter + 1}.png"
                table_path = os.path.join(output_folder, table_filename)
                pix.save(table_path)
                
                # Store metadata
                table_info = {
                    "type": "table",
                    "id": table_counter,
                    "filename": table_filename,
                    "path": table_path,
                    "url": f"file:///{os.path.abspath(table_path)}",
                    "page": page_num + 1,
                    "format": "png",
                    "size_bytes": os.path.getsize(table_path),
                    "width": pix.width,
                    "height": pix.height
                }
                
                all_data.append(table_info)
                table_counter += 1
                
                print(f"  + Extracted table: {table_filename} ({pix.width}x{pix.height})")
    
    pdf_document.close()
    
    print("\n" + "=" * 60)
    print(f"Extraction complete!")
    print(f"  - Embedded images extracted: {image_counter}")
    print(f"  - Tables extracted: {table_counter}")
    print(f"  - Items filtered out (too small): {filtered_count}")
    print(f"  - Total items found: {image_counter + table_counter + filtered_count}")
    print(f"All items saved to: {os.path.abspath(output_folder)}")
    
    # Save metadata
    metadata_path = os.path.join(output_folder, f"{pdf_name}_all_content_metadata.json")
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(all_data, f, indent=2)
    
    print(f"Metadata saved to: {metadata_path}")
    
    return all_data


def match_content_to_json(content_data, json_path):
    """
    Match extracted images/tables to questions in JSON based on page numbers.
    """
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    questions = data.get("JSON", {}).get("questions", [])
    
    for question in questions:
        page_start = question.get("page_start")
        page_end = question.get("page_end")
        
        # Find all content (images/tables) within the question's page range
        matching_content = [
            item for item in content_data
            if page_start <= item["page"] <= page_end
        ]
        
        # Separate by type
        images = [c for c in matching_content if c["type"] == "embedded_image"]
        tables = [c for c in matching_content if c["type"] == "table"]
        
        # Add to question
        question["image_urls"] = [img["url"] for img in images]
        question["image_paths"] = [img["path"] for img in images]
        question["image_count"] = len(images)
        
        question["table_urls"] = [tbl["url"] for tbl in tables]
        question["table_paths"] = [tbl["path"] for tbl in tables]
        question["table_count"] = len(tables)
    
    return data


if __name__ == "__main__":
    pdf_file = "AQA-8464C1H-QP-JUN22.PDF"
    
    # Filter settings
    MIN_WIDTH = 200
    MIN_HEIGHT = 200
    MIN_AREA = 40000
    
    if not os.path.exists(pdf_file):
        print(f"Error: PDF file '{pdf_file}' not found!")
        pdf_files = [f for f in os.listdir(".") if f.lower().endswith(".pdf")]
        if pdf_files:
            print(f"\nUsing first available PDF: {pdf_files[0]}")
            pdf_file = pdf_files[0]
        else:
            print("No PDF files found")
            exit(1)
    
    print(f"Filter settings: min_width={MIN_WIDTH}, min_height={MIN_HEIGHT}, min_area={MIN_AREA}")
    print("=" * 60)
    
    # Extract all content
    content_data = extract_tables_and_images_from_pdf(
        pdf_file,
        min_width=MIN_WIDTH,
        min_height=MIN_HEIGHT,
        min_area=MIN_AREA
    )
    
    # Match with JSON
    json_file = "past_paper.json"
    if os.path.exists(json_file):
        print("\n" + "=" * 60)
        print("Matching content to questions in JSON...")
        updated_data = match_content_to_json(content_data, json_file)
        
        output_json = "past_paper_with_all_content.json"
        with open(output_json, "w", encoding="utf-8") as f:
            json.dump(updated_data, f, indent=2)
        
        print(f"Updated JSON saved to: {output_json}")

