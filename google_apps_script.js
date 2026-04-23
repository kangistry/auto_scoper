/**
 * Google Apps Script to generate shareable links for all PDFs in a folder.
 * 
 * INSTRUCTIONS:
 * 1. Go to script.google.com and create a new project
 * 2. Paste this entire code
 * 3. Replace FOLDER_ID below with your Google Drive folder ID
 *    (The folder ID is in the URL when you open the folder: 
 *     https://drive.google.com/drive/folders/THIS_IS_THE_FOLDER_ID)
 * 4. Click Run → select "generatePdfLinks"
 * 5. Grant permissions when prompted
 * 6. Check your Google Drive for "pdf_links_mapping.csv"
 */

// Your Google Drive folder ID
const PARENT_FOLDER_ID = "17wv5AXZqeTYWmuFnicgvHMslL5xcW95-";

function generatePdfLinks() {
  const parentFolder = DriveApp.getFolderById(PARENT_FOLDER_ID);
  const results = [];
  
  // Add header
  results.push(["filename", "folder", "shareable_link", "direct_download_link"]);
  
  // Get all subfolders (the PDF folders)
  const subfolders = parentFolder.getFolders();
  
  while (subfolders.hasNext()) {
    const subfolder = subfolders.next();
    const folderName = subfolder.getName();
    
    Logger.log("Processing folder: " + folderName);
    
    // Get all PDF files in this subfolder
    const files = subfolder.getFilesByType(MimeType.PDF);
    
    while (files.hasNext()) {
      const file = files.next();
      const filename = file.getName();
      const fileId = file.getId();
      
      // Make file accessible to anyone with link
      file.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);
      
      // Generate links
      const shareableLink = "https://drive.google.com/file/d/" + fileId + "/view";
      const directDownloadLink = "https://drive.google.com/uc?export=download&id=" + fileId;
      
      results.push([filename, folderName, shareableLink, directDownloadLink]);
    }
  }
  
  // Create CSV content
  let csvContent = "";
  for (const row of results) {
    csvContent += row.map(cell => '"' + String(cell).replace(/"/g, '""') + '"').join(",") + "\n";
  }
  
  // Save to Drive
  const outputFile = DriveApp.createFile("pdf_links_mapping.csv", csvContent, MimeType.CSV);
  
  Logger.log("✅ Done! Created: " + outputFile.getUrl());
  Logger.log("Total PDFs processed: " + (results.length - 1));
  
  // Also create in the same parent folder for easy access
  parentFolder.addFile(outputFile);
  
  return outputFile.getUrl();
}

// Alternative: If you want to output to a Google Sheet instead
function generatePdfLinksToSheet() {
  const parentFolder = DriveApp.getFolderById(PARENT_FOLDER_ID);
  
  // Create new spreadsheet
  const ss = SpreadsheetApp.create("PDF Links Mapping");
  const sheet = ss.getActiveSheet();
  
  // Add header
  sheet.appendRow(["filename", "folder", "shareable_link", "direct_download_link"]);
  
  // Get all subfolders
  const subfolders = parentFolder.getFolders();
  let count = 0;
  
  while (subfolders.hasNext()) {
    const subfolder = subfolders.next();
    const folderName = subfolder.getName();
    const files = subfolder.getFilesByType(MimeType.PDF);
    
    while (files.hasNext()) {
      const file = files.next();
      const filename = file.getName();
      const fileId = file.getId();
      
      file.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);
      
      const shareableLink = "https://drive.google.com/file/d/" + fileId + "/view";
      const directDownloadLink = "https://drive.google.com/uc?export=download&id=" + fileId;
      
      sheet.appendRow([filename, folderName, shareableLink, directDownloadLink]);
      count++;
    }
  }
  
  Logger.log("✅ Done! Created spreadsheet: " + ss.getUrl());
  Logger.log("Total PDFs processed: " + count);
  
  return ss.getUrl();
}
