"""
Run Dify workflow for a single paper: C1H NOV21
"""
import cloudscraper
import json
import os
import sys

sys.stdout.reconfigure(encoding='utf-8')

# ============================================================
# CONFIGURATION
# ============================================================
API_KEY = "app-fd0HLVIwGDiY8QYvGdsbekQe"
BASE_URL = "https://dify.uplearn.co.uk/v1"

# Specific paper to process
PAPER_CODE = "C1H"
SESSION = "NOV21"
QP_FILE = "C1H Papers and Mark Schemes/AQA-8464C1H-QP-NOV21.PDF"
MS_FILE = "C1H Papers and Mark Schemes/AQA-8464C1H-MS-NOV21.PDF"
OUTPUT_JSON = "C1H_NOV21_result.json"

# Create a cloudscraper session
scraper = cloudscraper.create_scraper(
    browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
)

# Authentication Cookies
COOKIES = {
    "__Host-GCP_IAP_AUTH_TOKEN_220F89A4D796F67F": "AVBiXYjc1IzSKp8pfZCwMGG0QKRJn_3dsx3aLED0bv8ZyYIygjVnZsX1CwCXTCKSGRTW9dpk_Rk2VvMhwn7Ti2NHMEt1rAChclcCXAI5givarL2OYPJNbVkMDCZ2MbEFh6HTchfZhAEU4SRRLZHxNsip6CJsCBelBlubVWP19yjhcjboOyLuE7VPFZ_hGB_fAZSLezpey2ui_WdBU73jbgB0hBHkKl5XV62WizNnUN3jSUlHzWs78ng_46un3cnHpDlyvcmdkHGmE7SOVy_UJdF9KC6_-PoTn11M6G2H-767TTIroFq6J76w9cPi33p0Mqj_mdaGRso212DjOVyt__Nx3vfPbV1aCzIq3bSf1blVVnB8Z7ghCR0wL37JuLI326ChOUG0OxQ_uR7zTvX645yHgCCMIXZ5GBno6Ns-M_m5CGC3rB48Usx7-QudHu5WgXU-7hViyZQCnuL4lzZ7pPB8lU5OtkQuz2dFZndbDwkJ-79IB8B3w7X-dPmaGbQVCdjs8yPvARiOwt56szE9I3ujmtxq9f6abMdH-WXhNi5G9sdhyv8A95NmchQpFTMNC138e8Phivo_lY5FP6zty3x8MFwy_oYoJOO33WkFhZ1tARS92weqrrfirbh1CbcKNB0jTwxQSW3NOlsFXT3eW42laVQBr1QcZMB01GFTzxyI8Sdwe1CjKm5YLxNgnIalIwtRTM_D82oNjaMDM8PsmVxS5H0sPvzcSvFR9OVOHvEpnPVZsLhkiNiC-WiyBatl7dr8mL3gELYv8aY-JeDkCxiphh9M4wEvj6UZU_8gBvOv63h_2YO09IbVf3R0Hx5qfIpDUDc74U2qbDhFF-fTCr98AKvFXOtCppmCw8wLw_4j8cB-OuSJQzZP9A8uV4rZWgbzTQr6UsdqUfVI-dGeBBD_Z-sGuVAurrGI-pkaKaO569Td-A7kHmZWZXdkmAG4kEWKZ7Hf-1jfg7nTq1XmgX_47nRrbRPB6zpZctM6rQrANTbVx5boPMPURa-4zCmTzoFqm3G72-UNpegtku4lV41w78cFcvwcPiFss8S92kprM6g4MHbV8YexKNg7fGtYtklTNYODubb2pvgbaIB6D1yyPfCs3dKnV6Li4fvkC_CZa0phnDvkdFWJN8rgbk8u5maThYyWE1aMgswvPsVIlNyu7IOzG1I2o8zUUQFiw6xidGGl42Z-6YGsktOo5FyHRFoyJbAjfqg0_RsxO1hSoZ7kbDlPiJAtdwKLiWzPGKNOhM2q24EqrL10xFrkuVPKpiq2KBZIsVkP5o-U5Yfpc5MVxj6GGJzOQptvrMtAXh90z9tp51reqi7VEpsI7QMgD2GVZlvs9DD6o7tlAVhWhZN0TNCsmvZJBk4BtomdcLFQ4OkpcbDsqhlPXskm7jk5WapHzSJ2xKgeGhWxO5mYvRD-X3IVQOzd2y4Mjl9upTbcfbsJhVmRhP3eIPZoWCBbI9Ag48G0NU012757BkcEAV6Gmi2ox1Q6OOtapWh7BtQ-nVH8jnMSWgAo9fmNpIU4dp2BAntw_NH4GVCNkpN4VF2Qva__NxidPA_lPBzE-HaUaTE1ElsaO74OfvnexcukH2s_LLnedT3y1eGgsxMlIoGnxwD7ccWA-xt9cuc5gTEWDU-YxcQh-jhDYsdlondeJ6Zh0sL5xX1XXcM3iWPxwjCbrpGidj9cMIyBMg5SqLiE-J8FcWvvesnk_xCh6JC2eQSJ1bS407KpiF5ljd2Ax2Q2Zpk6EgNh_bYfKZ9UHLd4nS37CjWThwLtuZ_KA",
    "GCP_IAP_UID": "109661608269002925364",
    "auth-token": "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJ1cGxlYXJuLmNvLnVrIiwiZXhwIjoxNzcwMzk1MTkwLCJpYXQiOjE3Njc3MTY3OTAsImlzcyI6InVwbGVhcm4uY28udWsiLCJqdGkiOiJjYTk0NjkwMi01ODI2LTRiMGItYjkyYS1lNzFmZGIzZTYxZDEiLCJuYmYiOjE3Njc3MTY3ODksInN1YiI6IjQ0NTc4IiwidHlwIjoiYWNjZXNzIn0.yYCxKkqqPhRim7VDs1tBwi0T3HhCkWjraluGwP62RL5sZgXox5I0RGvUy5DFXQzJ5vFbLF2VGHFOaYkvrLYExQ",
    "locale": "en-US",
}

def get_mime_type(filename):
    ext = filename.lower().split('.')[-1]
    return {'pdf': 'application/pdf', 'png': 'image/png', 'jpg': 'image/jpeg', 'jpeg': 'image/jpeg'}.get(ext, 'application/octet-stream')

def upload_file(file_path, user_id="default-user"):
    url = f"{BASE_URL}/files/upload"
    headers = {"Authorization": f"Bearer {API_KEY}"}
    filename = os.path.basename(file_path)
    
    with open(file_path, 'rb') as f:
        files = {'file': (filename, f, get_mime_type(filename))}
        data = {'user': user_id}
        response = scraper.post(url, headers=headers, files=files, data=data, cookies=COOKIES)
    
    if response.status_code in [200, 201]:
        try:
            result = response.json()
            print(f"  Uploaded: {filename} -> ID: {result.get('id')}")
            return result
        except:
            print(f"  Error parsing response for {filename}")
            print(f"  Response: {response.text[:200]}...")
            return None
    else:
        print(f"  Error uploading {filename}: {response.status_code}")
        print(f"  {response.text[:200]}...")
        return None

def run_workflow_streaming(qp_ids, ms_ids, years="2021", user_id="default-user"):
    url = f"{BASE_URL}/workflows/run"
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    
    qp_files = [{"type": "document", "transfer_method": "local_file", "upload_file_id": fid} for fid in qp_ids]
    ms_files = [{"type": "document", "transfer_method": "local_file", "upload_file_id": fid} for fid in ms_ids]
    
    payload = {
        "inputs": {
            "Years_of_papers": years,
            "list_of_exam_papers": qp_files,
            "list_of_mark_schemes": ms_files
        },
        "response_mode": "streaming",
        "user": user_id
    }
    
    response = scraper.post(url, headers=headers, json=payload, cookies=COOKIES, stream=True)
    
    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        print(response.text[:500])
        return None
    
    final_result = None
    for line in response.iter_lines():
        if line:
            line_str = line.decode('utf-8')
            if line_str.startswith('data: '):
                try:
                    data = json.loads(line_str[6:])
                    event = data.get('event', '')
                    if event == 'node_finished':
                        node_title = data.get('data', {}).get('title', '')
                        print(f"  [Completed: {node_title}]")
                    elif event == 'workflow_finished':
                        final_result = data
                        print("  [Workflow finished]")
                except json.JSONDecodeError:
                    pass
    
    return final_result

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print("=" * 60)
    print(f"PROCESSING {PAPER_CODE} {SESSION}")
    print("=" * 60)
    print(f"QP: {QP_FILE}")
    print(f"MS: {MS_FILE}")
    
    # Upload files
    print("\nUploading files...")
    qp_result = upload_file(QP_FILE)
    ms_result = upload_file(MS_FILE)
    
    if not qp_result or not ms_result:
        print("Failed to upload files!")
        sys.exit(1)
    
    qp_id = qp_result.get('id')
    ms_id = ms_result.get('id')
    
    # Run workflow
    print("\nRunning workflow (streaming)...")
    result = run_workflow_streaming([qp_id], [ms_id], years="2021")
    
    if result:
        # Save result
        output_data = {
            "session": SESSION,
            "source_qp": QP_FILE,
            "source_ms": MS_FILE,
            "output": result.get('data', {}).get('outputs', {}).get('output', [])
        }
        
        with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n✓ Result saved to: {OUTPUT_JSON}")
    else:
        print("\nWorkflow failed or returned no result!")
        sys.exit(1)

