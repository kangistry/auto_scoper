"""
Clean up duplicate files in C1H Papers and Mark Schemes folder.
Renames files to remove "(1)", "(2)", etc. and removes true duplicates.
"""
import os
import re
import shutil

FOLDER = "C1H Papers and Mark Schemes"

def clean_filename(filename):
    """Remove (1), (2), etc. from filename and normalize."""
    # Remove the (N) pattern
    cleaned = re.sub(r'\s*\(\d+\)', '', filename)
    # Normalize case for extension
    name, ext = os.path.splitext(cleaned)
    return name + ext.upper()

def main():
    print("=" * 60)
    print("CLEANING UP C1H PAPERS FOLDER")
    print("=" * 60)
    
    files = os.listdir(FOLDER)
    print(f"Found {len(files)} files")
    print()
    
    # Group files by their cleaned name
    file_groups = {}
    for f in files:
        cleaned = clean_filename(f)
        if cleaned not in file_groups:
            file_groups[cleaned] = []
        file_groups[cleaned].append(f)
    
    # Process each group
    for cleaned_name, originals in sorted(file_groups.items()):
        if len(originals) == 1:
            # Single file - rename if needed
            original = originals[0]
            if original != cleaned_name:
                old_path = os.path.join(FOLDER, original)
                new_path = os.path.join(FOLDER, cleaned_name)
                if not os.path.exists(new_path):
                    os.rename(old_path, new_path)
                    print(f"Renamed: {original} -> {cleaned_name}")
                else:
                    print(f"Target exists, deleting: {original}")
                    os.remove(old_path)
            else:
                print(f"OK: {original}")
        else:
            # Multiple files - keep first, delete rest
            print(f"\nDuplicates found for: {cleaned_name}")
            # Sort to get consistent order
            originals.sort()
            
            # Keep the one without (N) if it exists, otherwise keep first
            keeper = None
            for orig in originals:
                if '(' not in orig:
                    keeper = orig
                    break
            if keeper is None:
                keeper = originals[0]
            
            for orig in originals:
                old_path = os.path.join(FOLDER, orig)
                if orig == keeper:
                    # Rename keeper to clean name if needed
                    if orig != cleaned_name:
                        new_path = os.path.join(FOLDER, cleaned_name)
                        if not os.path.exists(new_path):
                            os.rename(old_path, new_path)
                            print(f"  Kept and renamed: {orig} -> {cleaned_name}")
                        else:
                            print(f"  Target exists, deleting: {orig}")
                            os.remove(old_path)
                    else:
                        print(f"  Kept: {orig}")
                else:
                    os.remove(old_path)
                    print(f"  Deleted: {orig}")
    
    print()
    print("=" * 60)
    print("CLEANUP COMPLETE")
    print("=" * 60)
    
    # Show final contents
    print("\nFinal folder contents:")
    for f in sorted(os.listdir(FOLDER)):
        print(f"  {f}")

if __name__ == "__main__":
    main()

