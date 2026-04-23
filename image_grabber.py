import fitz  # PyMuPDF
import os
import json
from pathlib import Path

def extract_images_from_pdf(pdf_path, output_folder="extracted_images", 
                           min_width=200, min_height=200, min_area=40000):
    """
    Extract all images from a PDF file and save them with metadata.
    
    Args:
        pdf_path: Path to the PDF file
        output_folder: Folder to save extracted images
        min_width: Minimum width in pixels (default: 200)
        min_height: Minimum height in pixels (default: 200)
        min_area: Minimum area in pixels (width * height, default: 40000)
    
    Returns:
        Dictionary with image information and URLs/paths
    """
    # Create output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    
    # Get the PDF filename without extension for naming
    pdf_name = Path(pdf_path).stem
    
    # Open the PDF
    pdf_document = fitz.open(pdf_path)
    
    image_data = []
    image_counter = 0
    
    print(f"Processing: {pdf_path}")
    print(f"Total pages: {len(pdf_document)}")
    print("-" * 60)
    
    # Track filtered images
    filtered_count = 0
    
    # Iterate through each page
    for page_num in range(len(pdf_document)):
        page = pdf_document[page_num]
        
        # Get images on the page
        image_list = page.get_images(full=True)
        
        if image_list:
            print(f"\nPage {page_num + 1}: Found {len(image_list)} image(s)")
        
        # Extract each image
        for img_index, img in enumerate(image_list):
            # Get the XREF of the image
            xref = img[0]
            
            # Extract the image bytes
            base_image = pdf_document.extract_image(xref)
            image_bytes = base_image["image"]
            image_ext = base_image["ext"]
            width = base_image["width"]
            height = base_image["height"]
            area = width * height
            
            # Filter out small images
            if width < min_width or height < min_height or area < min_area:
                print(f"  - Skipped: {width}x{height} (too small)")
                filtered_count += 1
                continue
            
            # Generate filename
            image_filename = f"{pdf_name}_page{page_num + 1}_img{img_index + 1}.{image_ext}"
            image_path = os.path.join(output_folder, image_filename)
            
            # Save the image
            with open(image_path, "wb") as image_file:
                image_file.write(image_bytes)
            
            # Store metadata
            image_info = {
                "image_id": image_counter,
                "filename": image_filename,
                "path": image_path,
                "url": f"file:///{os.path.abspath(image_path)}",
                "page": page_num + 1,
                "format": image_ext,
                "size_bytes": len(image_bytes),
                "width": width,
                "height": height
            }
            
            image_data.append(image_info)
            image_counter += 1
            
            print(f"  - Extracted: {image_filename} ({width}x{height}, {image_ext})")
    
    pdf_document.close()
    
    print("\n" + "=" * 60)
    print(f"Extraction complete!")
    print(f"  - Images extracted: {image_counter}")
    print(f"  - Images filtered out (too small): {filtered_count}")
    print(f"  - Total images found: {image_counter + filtered_count}")
    print(f"Images saved to: {os.path.abspath(output_folder)}")
    
    # Save metadata to JSON
    metadata_path = os.path.join(output_folder, f"{pdf_name}_image_metadata.json")
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(image_data, f, indent=2)
    
    print(f"Metadata saved to: {metadata_path}")
    
    return image_data


def match_images_to_json(image_data, json_path):
    """
    Match extracted images to questions in the JSON based on page numbers.
    
    Args:
        image_data: List of image metadata dictionaries
        json_path: Path to the past paper JSON file
    
    Returns:
        Updated JSON with image URLs added to questions
    """
    # Load the JSON
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    questions = data.get("JSON", {}).get("questions", [])
    
    # Match images to questions based on page numbers
    for question in questions:
        page_start = question.get("page_start")
        page_end = question.get("page_end")
        
        # Find images that fall within the question's page range
        matching_images = [
            img for img in image_data
            if page_start <= img["page"] <= page_end
        ]
        
        # Add image URLs to the question
        question["image_urls"] = [img["url"] for img in matching_images]
        question["image_paths"] = [img["path"] for img in matching_images]
        question["image_count"] = len(matching_images)
    
    return data


if __name__ == "__main__":
    # Example usage - Configure these parameters as needed
    pdf_file = "AQA-8464C1H-QP-JUN22.PDF"  # Change this to your PDF file
    
    # Filter settings - adjust these to control which images are extracted
    MIN_WIDTH = 200      # Minimum width in pixels
    MIN_HEIGHT = 200     # Minimum height in pixels  
    MIN_AREA = 40000     # Minimum area (width * height)
    
    if not os.path.exists(pdf_file):
        print(f"Error: PDF file '{pdf_file}' not found!")
        print(f"Current directory: {os.getcwd()}")
        print("\nAvailable PDF files:")
        pdf_files = [f for f in os.listdir(".") if f.lower().endswith(".pdf")]
        if pdf_files:
            for pdf in pdf_files:
                print(f"  - {pdf}")
            print(f"\nUsing first available PDF: {pdf_files[0]}")
            pdf_file = pdf_files[0]
        else:
            print("  No PDF files found in current directory")
            exit(1)
    
    print(f"Filter settings: min_width={MIN_WIDTH}, min_height={MIN_HEIGHT}, min_area={MIN_AREA}")
    print("=" * 60)
    
    # Extract images with filters
    image_data = extract_images_from_pdf(
        pdf_file, 
        min_width=MIN_WIDTH,
        min_height=MIN_HEIGHT,
        min_area=MIN_AREA
    )
    
    # Optional: Match with JSON if it exists
    json_file = "past_paper.json"
    if os.path.exists(json_file):
        print("\n" + "=" * 60)
        print("Matching images to questions in JSON...")
        updated_data = match_images_to_json(image_data, json_file)
        
        # Save updated JSON
        output_json = "past_paper_with_images.json"
        with open(output_json, "w", encoding="utf-8") as f:
            json.dump(updated_data, f, indent=2)
        
        print(f"Updated JSON saved to: {output_json}")

