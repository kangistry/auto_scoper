import fitz  # PyMuPDF
import os
import json
from pathlib import Path

def extract_with_json_guidance(pdf_path, json_path, output_folder="extracted_content",
                                min_width=200, min_height=200, min_area=40000):
    """
    Extract images and tables from PDF using JSON metadata to guide extraction.
    
    This uses the JSON file to know exactly which pages have figures/tables,
    then extracts them appropriately.
    """
    # Load JSON to know which pages have what
    with open(json_path, "r", encoding="utf-8") as f:
        json_data = json.load(f)
    
    questions = json_data.get("JSON", {}).get("questions", [])
    
    # Build a map of pages to their content types
    page_info = {}
    for q in questions:
        for page in range(q["page_start"], q["page_end"] + 1):
            if page not in page_info:
                page_info[page] = {"has_figure": False, "has_table": False, "labels": []}
            
            if q.get("has_figure") == "true":
                page_info[page]["has_figure"] = True
                if q.get("figure_labels_joined"):
                    page_info[page]["labels"].append(q["figure_labels_joined"])
            
            if q.get("has_table") == "true":
                page_info[page]["has_table"] = True
                if q.get("table_labels_joined"):
                    page_info[page]["labels"].append(q["table_labels_joined"])
    
    # Create output folder
    os.makedirs(output_folder, exist_ok=True)
    
    pdf_name = Path(pdf_path).stem
    pdf_document = fitz.open(pdf_path)
    
    all_content = []
    image_counter = 0
    table_counter = 0
    filtered_count = 0
    
    print(f"Processing: {pdf_path}")
    print(f"Guided by: {json_path}")
    print(f"Total pages: {len(pdf_document)}")
    print("=" * 60)
    
    # Process each page
    for page_num in range(len(pdf_document)):
        page = pdf_document[page_num]
        page_number = page_num + 1
        
        info = page_info.get(page_number, {})
        has_fig = info.get("has_figure", False)
        has_tbl = info.get("has_table", False)
        labels = info.get("labels", [])
        
        if has_fig or has_tbl:
            status = []
            if has_fig:
                status.append("Figure")
            if has_tbl:
                status.append("Table")
            label_str = f" ({', '.join(labels)})" if labels else ""
            print(f"\nPage {page_number}: {'/'.join(status)}{label_str}")
        
        # Extract embedded images
        image_list = page.get_images(full=True)
        
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
                filtered_count += 1
                continue
            
            # Determine content type based on page info
            content_type = "image"
            if has_tbl and not has_fig:
                content_type = "table"
            elif has_fig and not has_tbl:
                content_type = "figure"
            elif has_fig and has_tbl:
                # Both on same page - we'll label it generically
                content_type = "figure_or_table"
            
            # Save the image
            filename = f"{pdf_name}_page{page_number}_{content_type}{image_counter + 1}.{image_ext}"
            filepath = os.path.join(output_folder, filename)
            
            with open(filepath, "wb") as f:
                f.write(image_bytes)
            
            # Store metadata
            metadata = {
                "type": content_type,
                "id": image_counter,
                "filename": filename,
                "path": filepath,
                "url": f"file:///{os.path.abspath(filepath)}",
                "page": page_number,
                "format": image_ext,
                "size_bytes": len(image_bytes),
                "width": width,
                "height": height,
                "labels": labels
            }
            
            all_content.append(metadata)
            image_counter += 1
            
            print(f"  + Extracted {content_type}: {filename} ({width}x{height})")
        
        # If page is supposed to have table but no embedded image found,
        # capture the page region as an image
        if has_tbl and not image_list:
            print(f"  > Table expected but no embedded image found")
            print(f"  > Capturing page region as table image...")
            
            # Get text blocks to find table region
            blocks = page.get_text("dict")["blocks"]
            text_blocks = [b for b in blocks if b.get("type") == 0]  # 0 = text
            
            if text_blocks:
                # Find bounding box of text (likely contains table)
                x0 = min(b["bbox"][0] for b in text_blocks)
                y0 = min(b["bbox"][1] for b in text_blocks)
                x1 = max(b["bbox"][2] for b in text_blocks)
                y1 = max(b["bbox"][3] for b in text_blocks)
                
                table_rect = fitz.Rect(x0, y0, x1, y1)
                table_rect = table_rect + (-10, -10, 10, 10)  # Add margin
                
                # Render as image
                mat = fitz.Matrix(2, 2)  # 2x zoom
                pix = page.get_pixmap(matrix=mat, clip=table_rect)
                
                filename = f"{pdf_name}_page{page_number}_table{table_counter + 1}.png"
                filepath = os.path.join(output_folder, filename)
                pix.save(filepath)
                
                metadata = {
                    "type": "table",
                    "id": table_counter,
                    "filename": filename,
                    "path": filepath,
                    "url": f"file:///{os.path.abspath(filepath)}",
                    "page": page_number,
                    "format": "png",
                    "size_bytes": os.path.getsize(filepath),
                    "width": pix.width,
                    "height": pix.height,
                    "labels": labels
                }
                
                all_content.append(metadata)
                table_counter += 1
                
                print(f"  + Captured table: {filename} ({pix.width}x{pix.height})")
    
    pdf_document.close()
    
    print("\n" + "=" * 60)
    print(f"Extraction complete!")
    print(f"  - Total items extracted: {len(all_content)}")
    print(f"  - Items filtered out (too small): {filtered_count}")
    print(f"Items saved to: {os.path.abspath(output_folder)}")
    
    # Save metadata
    metadata_path = os.path.join(output_folder, f"{pdf_name}_metadata.json")
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(all_content, f, indent=2)
    
    print(f"Metadata saved to: {metadata_path}")
    
    return all_content


if __name__ == "__main__":
    pdf_file = "AQA-8464C1H-QP-JUN22.PDF"
    json_file = "past_paper.json"
    
    # Filter settings
    MIN_WIDTH = 200
    MIN_HEIGHT = 200
    MIN_AREA = 40000
    
    if not os.path.exists(pdf_file) or not os.path.exists(json_file):
        print(f"Error: Required files not found!")
        print(f"  PDF: {pdf_file} - {'Found' if os.path.exists(pdf_file) else 'NOT FOUND'}")
        print(f"  JSON: {json_file} - {'Found' if os.path.exists(json_file) else 'NOT FOUND'}")
        exit(1)
    
    print(f"Filter settings: min_width={MIN_WIDTH}, min_height={MIN_HEIGHT}, min_area={MIN_AREA}")
    print("=" * 60)
    
    # Extract with JSON guidance
    content = extract_with_json_guidance(
        pdf_file,
        json_file,
        min_width=MIN_WIDTH,
        min_height=MIN_HEIGHT,
        min_area=MIN_AREA
    )
    
    print(f"\nExtracted {len(content)} items")
    
    # Count by type
    types = {}
    for item in content:
        t = item["type"]
        types[t] = types.get(t, 0) + 1
    
    print("\nBreakdown by type:")
    for t, count in types.items():
        print(f"  - {t}: {count}")


