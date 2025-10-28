import json
from poly_utils.google_utils import get_spreadsheet
import pandas as pd 
import os


def pretty_print(txt, dic):
    print("\n", txt, json.dumps(dic, indent=4))


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Trim whitespace and normalise column names."""
    if df.empty:
        return df

    renamed = {}
    for col in df.columns:
        if isinstance(col, str):
            normalized = " ".join(col.strip().split())
            renamed[col] = normalized
        else:
            renamed[col] = col
    return df.rename(columns=renamed)


def _ensure_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Ensure dataframe contains required column (case-insensitive)."""
    if column in df.columns:
        return df

    column_lower = column.lower()
    for existing in df.columns:
        if isinstance(existing, str) and existing.lower() == column_lower:
            return df.rename(columns={existing: column})

    raise KeyError(f"Required column '{column}' not found in sheet data. Columns: {list(df.columns)}")


def get_sheet_df(read_only=None):
    """
    Get sheet data with optional read-only mode
    
    Args:
        read_only (bool): If None, auto-detects based on credentials availability
    """
    all = 'All Markets'
    sel = 'Selected Markets'

    # Auto-detect read-only mode if not specified
    if read_only is None:
        creds_file = 'credentials.json' if os.path.exists('credentials.json') else '../credentials.json'
        read_only = not os.path.exists(creds_file)
        if read_only:
            print("No credentials found, using read-only mode")

    try:
        spreadsheet = get_spreadsheet(read_only=read_only)
    except FileNotFoundError:
        print("No credentials found, falling back to read-only mode")
        spreadsheet = get_spreadsheet(read_only=True)

    wk = spreadsheet.worksheet(sel)
    df = pd.DataFrame(wk.get_all_records())
    df = _normalize_columns(df)
    df = _ensure_column(df, 'question')
    df = df[df['question'] != ""].reset_index(drop=True)

    wk2 = spreadsheet.worksheet(all)
    df2 = pd.DataFrame(wk2.get_all_records())
    df2 = _normalize_columns(df2)
    df2 = _ensure_column(df2, 'question')
    df2 = df2[df2['question'] != ""].reset_index(drop=True)

    result = df.merge(df2, on='question', how='inner')

    wk_p = spreadsheet.worksheet('Hyperparameters')
    records = wk_p.get_all_records()
    hyperparams, current_type = {}, None

    for r in records:
        # Update current_type only when we have a non-empty type value
        # Handle both string and NaN values from pandas
        type_value = r['type']
        if type_value and str(type_value).strip() and str(type_value) != 'nan':
            current_type = str(type_value).strip()
        
        # Skip rows where we don't have a current_type set
        if current_type:
            # Convert numeric values to appropriate types
            value = r['value']
            try:
                # Try to convert to float if it's numeric
                if isinstance(value, str) and value.replace('.', '').replace('-', '').isdigit():
                    value = float(value)
                elif isinstance(value, (int, float)):
                    value = float(value)
            except (ValueError, TypeError):
                pass  # Keep as string if conversion fails
            
            hyperparams.setdefault(current_type, {})[r['param']] = value

    return result, hyperparams
