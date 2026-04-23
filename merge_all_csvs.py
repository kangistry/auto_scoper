import pandas as pd
import os

# List of all CSV files to merge
csv_files = [
    "C1F_papers_combined.csv",
    "C1H_papers_combined.csv", 
    "C1H_NOV21.csv",
    "C2F_papers_combined.csv",
    "C2H Papers.csv",
    "C1F_JUN22_papers.csv"
]

print("Merging CSV files...")
print("=" * 60)

# Read and combine all CSVs
all_dfs = []
for csv_file in csv_files:
    if os.path.exists(csv_file):
        df = pd.read_csv(csv_file, encoding='utf-8-sig')
        print(f"  {csv_file}: {len(df)} rows")
        all_dfs.append(df)
    else:
        print(f"  Warning: {csv_file} not found, skipping...")

# Combine all dataframes
combined_df = pd.concat(all_dfs, ignore_index=True)

# Sort by exam type, year, session, question_id, subquestion_id for nice ordering
combined_df = combined_df.sort_values(
    by=['exam', 'year', 'session', 'question_id', 'subquestion_id'],
    ascending=[True, True, True, True, True]
)

# Save to new CSV
output_file = "ALL_PAPERS_COMBINED.csv"
combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')

print("=" * 60)
print(f"\nMerged {len(all_dfs)} CSV files")
print(f"Total rows: {len(combined_df)}")
print(f"Output saved to: {output_file}")

# Summary by paper type
print("\n" + "=" * 60)
print("Summary by exam:")
summary = combined_df.groupby(['exam', 'year', 'session']).size().reset_index(name='questions')
for _, row in summary.iterrows():
    print(f"  {row['session']} {row['year']} - {row['exam'][:40]}... : {row['questions']} questions")

print("\n" + "=" * 60)
print("Done!")
