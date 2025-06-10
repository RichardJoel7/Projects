import pandas as pd

# Step 1: Read CSVs with encoding to handle BOM
mozenda_df = pd.read_csv('MozendaTestOutput.csv', encoding='utf-8-sig')
le_df = pd.read_csv('MLETestOutput.csv', encoding='utf-8-sig')

# Debug: Check column names
print("Mozenda Columns:", mozenda_df.columns.tolist())
print("LE Columns:", le_df.columns.tolist())

# Step 2: Clean column names
mozenda_df.columns = mozenda_df.columns.str.strip().str.replace('\ufeff', '')
le_df.columns = le_df.columns.str.strip().str.replace('\ufeff', '')

# Step 3: Normalize strings (lowercase, strip spaces)
def clean_text(x):
    return str(x).strip().lower()

mozenda_df['Person Name'] = mozenda_df['Person Name'].apply(clean_text)
mozenda_df['Person Position'] = mozenda_df['Person Position'].apply(clean_text)
le_df['Person Name'] = le_df['Person Name'].apply(clean_text)
le_df['Person Position'] = le_df['Person Position'].apply(clean_text)

# Step 4: Group and aggregate sets + counts
mozenda_grouped = mozenda_df.groupby('MatrixID').agg({
    'Person Name': lambda x: set(x),
    'Person Position': lambda x: set(x)
}).rename(columns={
    'Person Name': 'Names_Mozenda',
    'Person Position': 'Positions_Mozenda'
})
mozenda_grouped['Person Count_Mozenda'] = mozenda_grouped['Names_Mozenda'].apply(len)

le_grouped = le_df.groupby('MatrixID').agg({
    'Person Name': lambda x: set(x),
    'Person Position': lambda x: set(x)
}).rename(columns={
    'Person Name': 'Names_LE',
    'Person Position': 'Positions_LE'
})
le_grouped['Person Count_LE'] = le_grouped['Names_LE'].apply(len)

# Step 5: Merge the two datasets
merged = pd.merge(mozenda_grouped, le_grouped, left_index=True, right_index=True, how='outer')

# Step 6: Compare fields
merged['Name Match'] = merged.apply(
    lambda row: row['Names_Mozenda'] == row['Names_LE']
    if pd.notna(row['Names_Mozenda']) and pd.notna(row['Names_LE']) else False, axis=1)

merged['Position Match'] = merged.apply(
    lambda row: row['Positions_Mozenda'] == row['Positions_LE']
    if pd.notna(row['Positions_Mozenda']) and pd.notna(row['Positions_LE']) else False, axis=1)

# Step 7: Fill missing count and match columns only
merged['Person Count_Mozenda'] = merged['Person Count_Mozenda'].fillna(0).astype(int)
merged['Person Count_LE'] = merged['Person Count_LE'].fillna(0).astype(int)
merged['Name Match'] = merged['Name Match'].fillna(False)
merged['Position Match'] = merged['Position Match'].fillna(False)

# Step 8: Final Output
final_df = merged.reset_index()[[
    'MatrixID', 'Person Count_Mozenda', 'Person Count_LE', 'Name Match', 'Position Match'
]]

# Step 9: Export to CSV
final_df.to_csv('ComparisonOutput.csv', index=False)
print("✅ Comparison completed and saved to 'ComparisonOutput.csv'")