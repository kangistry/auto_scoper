"""
Quick script to process a single paper pair
"""
import cloudscraper
import json
import os
import csv

# Import from main workflow
import sys
sys.path.insert(0, '.')

# ============================================================
# CONFIGURATION - Set the specific paper to process
# ============================================================
PAPER_CODE = "C1F"
QP_FILE = "C1F Papers and Mark Schemes/AQA-8464C1F-QP-JUN22.PDF"
MS_FILE = "C1F Papers and Mark Schemes/AQA-8464-C1F-Final-MS-Jun22-v1.1.pdf"
SESSION = "JUN22"
# ============================================================

# API Configuration
API_KEY = "app-fd0HLVIwGDiY8QYvGdsbekQe"
BASE_URL = "https://dify.uplearn.co.uk/v1"

# Create cloudscraper session
scraper = cloudscraper.create_scraper(
    browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
)

# Authentication Cookies (copy from dify_workflow.py)
COOKIES = {
    "__Host-GCP_IAP_AUTH_TOKEN_220F89A4D796F67F": "AVBiXYjEI8cC8hszITkOkNRRLvd6mQZUd3t2cDBv9AG2u67js7Tk7wyXdc0-DxmI8AcE3xW-tddeOwqed--unVZ9-DbC1OIHrBPmGR2_JbUyuo2oufaoFjvP0yPcAsZoZidKmhGs-1FNWqs1A29m29lG3JGtITNd3vxLD9Wp0twRyF6r72UB-BEkBgYAePprjw1mBG5DmrxpyHhyM2hrWUYNIwRkAEPR2kfg4ONGPiAozB3gKoLGtpHKLghm9xC91udky9YKoIlgIw3eD5OWaUr_uSMAlK486kVHD9NUexqdo9I23TXIwgVoXWRg-I7_Fk5RnJIj-ssThfG4eCleIbN-kruUbYrwmdMnpSaePxSgRRpldRiP8sdFFbKoJrQ8WfDE8UPNb7vE-wTAVd1RLdo0VtpKpVOoyU7S1UVRSp_fXuMsuWXq_CPNaIwDdmI5O52uqYn9g-38jRMB3FB90tXSAa5i9yRaj6PG-G9KzxYM_fI2_-f5YbNAt2DrLbjY_HhkiC9IyOs6JMaqaeL4wwGmr1E61h7Y7hR2lMv3D4zJVE1wtWcxHJnb3-FA0Z02sljv485rPS9-OykQMA4jzlJeodmzs-jB2bjro8_tcyCR3nXXJfCR24Vnbw66n7jiai-iUCU91ksQ1u88robplU4yECO6NAeZq8c0r7uR-AtQi-M6eF92F7nlsH_dZmUaST8jSa6MevscxI6FO0SGIg7-iJ-lrl9g2bj_2LwGvM1WfNdPHDnv8ZOXHLapgW7Wa4buRmBf81hHbQUOl3nVUTZU280vZYGAnjmSlTogDDdPcAxoldY7s7f4DU3_rnTT54vWyrYFQWMXATGCTnw1hdG1KTr8dMfIOMJfhh7wVg_3rrP3dTll8l1zL9Wd6JPddU1Xn-VqD5N8DDP5q4P2Z-Nl3JV0Lph5IuqW93EFzVtD6OnTaYLwKNqyVUl6VaqGLBTqFqBXp6Ed8Iq1-154uHv6qmum6il4nQMHEAC13g3Z8Vx9sq1ngoizGtAYoHwKaUmSLuoCOk8id_H0LSzxTaPOd16v1xC0TYQ5ZeAMz-ptZQvSgD35dh-mS5a-ieVVGvZioskf1p9v-lgLM2illXgZ_iOrh_E3lwJTIu97I-tgP9u7J37pCkh2ZI-6MKn1WnJAIm3FhvcUr7nzAtZAVEcmkLCO604LUn4UHfp7ONNvvfK1tk_77K9FIw1Tz0w-FD92bjaaQ_8ehnLgyvOVIxJIGR-H2fboHIi9e9nVgr70ncecESD25EFUzQgu0gHmqqe2ZIAlk-9rlaRzEFNwfsJwJgbo8KG5XtiFvb0V7rbfenmdwkHjVXF9Kqd_P95cVq8RWL5iUN9_GHt0oc6nslvaiUcacrV1fLThPAmLwqsqe48xHebmc0rGm8pKC8seVXYKBqBbBVZxjM_VyWhFQaAYZU_BUdTQn73m59HUPyAuMusE2baElTXfM0FunHDP1zN4mgDgDU79ylnFcPQ6EGvrBdavuC3arvC2jwQ3pkUqKVEpR_RtiT7SqCh3crjLqPFhUypsk_NRwStUShjNCSoTJk5nAcKQR5ydaZD0ROjWgnHqwA_EUrM-FX0xSyX7QILxT2q9CKlL8pEX6Vb1rTKSg73vagn6uB7F88EhDP2JVQAbGl4aL4DMOmFVN47lPH-euN_T6OqLrKB0vkVss4Z8Z1taI_qk_J7JAEtq5esN1kZR0LBO5D4DAIUTTtqWC7h_BRjv8TArn5yWBQ",
    "GCP_IAP_UID": "109661608269002925364",
    "auth-token": "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJ1cGxlYXJuLmNvLnVrIiwiZXhwIjoxNzcwMzk1MTkwLCJpYXQiOjE3Njc3MTY3OTAsImlzcyI6InVwbGVhcm4uY28udWsiLCJqdGkiOiJjYTk0NjkwMi01ODI2LTRiMGItYjkyYS1lNzFmZGIzZTYxZDEiLCJuYmYiOjE3Njc3MTY3ODksInN1YiI6IjQ0NTc4IiwidHlwIjoiYWNjZXNzIn0.yYCxKkqqPhRim7VDs1tBwi0T3HhCkWjraluGwP62RL5sZgXox5I0RGvUy5DFXQzJ5vFbLF2VGHFOaYkvrLYExQ",
    "locale": "en-US",
}

def get_mime_type(filename):
    ext = filename.lower().split('.')[-1]
    mime_types = {'pdf': 'application/pdf', 'json': 'application/json'}
    return mime_types.get(ext, 'application/octet-stream')

def upload_file(file_path, user_id="default-user"):
    url = f"{BASE_URL}/files/upload"
    headers = {"Authorization": f"Bearer {API_KEY}"}
    filename = os.path.basename(file_path)
    
    with open(file_path, 'rb') as f:
        files = {'file': (filename, f, get_mime_type(filename))}
        data = {'user': user_id}
        response = scraper.post(url, headers=headers, files=files, data=data, cookies=COOKIES)
    
    print(f"  Response status: {response.status_code}")
    
    if response.status_code in [200, 201]:
        try:
            result = response.json()
            print(f"  Uploaded: {filename} -> ID: {result.get('id')}")
            return result
        except Exception as e:
            print(f"  Error parsing response: {e}")
            print(f"  Response text: {response.text[:500]}")
            return None
    else:
        print(f"  Error uploading {filename}: {response.status_code}")
        print(f"  Response: {response.text[:1000]}")
        return None

def run_workflow_streaming(inputs, qp_file, ms_file, user_id="default-user"):
    url = f"{BASE_URL}/workflows/run"
    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
    
    # Add file references to inputs
    inputs["list_of_exam_papers"] = [{"type": "document", "transfer_method": "local_file", "upload_file_id": qp_file.get("id")}]
    inputs["list_of_mark_schemes"] = [{"type": "document", "transfer_method": "local_file", "upload_file_id": ms_file.get("id")}]
    
    payload = {"inputs": inputs, "response_mode": "streaming", "user": user_id}
    
    print("\nRunning workflow (streaming)...")
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
    return final_result if final_result else {"response": full_response}


if __name__ == "__main__":
    print("=" * 60)
    print(f"SINGLE PAPER PROCESSOR - {PAPER_CODE} {SESSION}")
    print("=" * 60)
    print(f"QP: {QP_FILE}")
    print(f"MS: {MS_FILE}")
    
    # Check files exist
    if not os.path.exists(QP_FILE):
        print(f"Error: QP file not found: {QP_FILE}")
        exit(1)
    if not os.path.exists(MS_FILE):
        print(f"Error: MS file not found: {MS_FILE}")
        exit(1)
    
    # Upload files
    print("\n" + "=" * 60)
    print("UPLOADING FILES")
    print("=" * 60)
    
    uploaded_qp = upload_file(QP_FILE)
    uploaded_ms = upload_file(MS_FILE)
    
    if not uploaded_qp or not uploaded_ms:
        print("Error: Failed to upload files")
        exit(1)
    
    # Run workflow
    print("\n" + "=" * 60)
    print("RUNNING WORKFLOW")
    print("=" * 60)
    
    workflow_inputs = {"Years_of_papers": SESSION}
    result = run_workflow_streaming(workflow_inputs, uploaded_qp, uploaded_ms, user_id="exam-indexer")
    
    if result:
        # Save result
        output_file = f"{PAPER_CODE}_{SESSION}_result.json"
        
        # Wrap in the expected format for compatibility with existing JSON
        wrapped_result = [{
            'session': SESSION,
            'qp_file': QP_FILE,
            'ms_file': MS_FILE,
            'result': result
        }]
        
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(wrapped_result, f, indent=2)
        
        print(f"\n" + "=" * 60)
        print("SUCCESS!")
        print("=" * 60)
        print(f"Result saved to: {output_file}")
        print(f"\nTo add to existing {PAPER_CODE} results, manually merge with {PAPER_CODE}_workflow_results.json")
    else:
        print("\nWorkflow failed!")

