import pandas as pd
import logging
from datetime import timedelta

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define ES modes with thresholds
ES_MODES = {
    'conservative': {'DL_Prb_Utilization': 10, 'Avg_UE_Number': 10},
    'moderate': {'DL_Prb_Utilization': 15, 'Avg_UE_Number': 13},
    'aggressive': {'DL_Prb_Utilization': 20, 'Avg_UE_Number': 17}
}

# Function to get user input for ES mode
def get_es_mode():
    print("Available Energy Saving Modes:")
    for mode in ES_MODES:
        print(f"- {mode.capitalize()}: DL_Prb_Utilization <= {ES_MODES[mode]['DL_Prb_Utilization']}, "
              f"Avg_UE_Number <= {ES_MODES[mode]['Avg_UE_Number']}")
    while True:
        mode = input("Enter ES mode (conservative/moderate/aggressive): ").lower()
        if mode in ES_MODES:
            return mode
        print("Invalid mode. Please choose 'conservative', 'moderate', or 'aggressive'.")

# Load forecasted KPI data
try:
    df = pd.read_csv('forecast_kpis.csv')
except FileNotFoundError:
    logging.error("Forecasted KPI file not found. Ensure 'forecast_kpis.csv' is in D:\\HP.")
    exit()

# Verify required columns
required_cols = ['Timestamp', 'NCI', 'gNB', 'Avg_UE_Number', 'DL_Prb_Utilization']
if not all(col in df.columns for col in required_cols):
    logging.error(f"CSV must contain columns: {required_cols}")
    exit()

# Convert Timestamp to datetime
try:
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
except Exception as e:
    logging.error(f"Error parsing timestamps: {e}")
    exit()

# Check for NaT values
if df['Timestamp'].isna().any():
    logging.warning("Some timestamps could not be parsed.")
    logging.info("Rows with invalid timestamps:\n%s", df[df['Timestamp'].isna()])
    exit()

# Verify data for all expected NCIs
expected_ncis = {357783981, 357783979, 357783980, 358531244, 358531245, 358531243}
actual_ncis = set(df['NCI'].unique())
if not expected_ncis.issubset(actual_ncis):
    logging.warning(f"Missing NCIs in forecast data. Expected: {expected_ncis}, Found: {actual_ncis}")

# Get user input for ES mode
es_mode = get_es_mode()
min_window_intervals = 3  # Fixed 30 minutes (3 x 15-minute intervals)

# Function to identify shutdown windows
def find_shutdown_windows(df, mode, min_intervals):
    shutdown_windows = []
    thresholds = ES_MODES[mode]
    
    for nci in df['NCI'].unique():
        df_cell = df[df['NCI'] == nci][['Timestamp', 'Avg_UE_Number', 'DL_Prb_Utilization', 'gNB']].sort_values('Timestamp')
        if df_cell.empty:
            logging.warning(f"No data for NCI {nci}")
            continue
        
        gnb = df_cell['gNB'].iloc[0]
        
        # Identify intervals where both KPIs meet the mode's thresholds
        df_cell['shutdown_eligible'] = (
            (df_cell['Avg_UE_Number'] <= thresholds['Avg_UE_Number']) &
            (df_cell['DL_Prb_Utilization'] <= thresholds['DL_Prb_Utilization'])
        )
        
        # Find consecutive eligible intervals
        start_idx = None
        for i in range(len(df_cell)):
            if df_cell['shutdown_eligible'].iloc[i]:
                if start_idx is None:
                    start_idx = i
            else:
                if start_idx is not None:
                    end_idx = i - 1
                    if end_idx - start_idx + 1 >= min_intervals:
                        start_time = df_cell['Timestamp'].iloc[start_idx]
                        end_time = df_cell['Timestamp'].iloc[end_idx]
                        duration = (end_time - start_time).total_seconds() / 60
                        shutdown_windows.append({
                            'Start_Timestamp': start_time,
                            'End_Timestamp': end_time,
                            'NCI': nci,
                            'gNB': gnb,
                            'Duration_Minutes': duration
                        })
                    start_idx = None
        
        # Check if the last window is eligible
        if start_idx is not None and len(df_cell) - start_idx >= min_intervals:
            start_time = df_cell['Timestamp'].iloc[start_idx]
            end_time = df_cell['Timestamp'].iloc[-1]
            duration = (end_time - start_time).total_seconds() / 60
            shutdown_windows.append({
                'Start_Timestamp': start_time,
                'End_Timestamp': end_time,
                'NCI': nci,
                'gNB': gnb,
                'Duration_Minutes': duration
            })
    
    return pd.DataFrame(shutdown_windows)

# Identify shutdown windows
shutdown_df = find_shutdown_windows(df, es_mode, min_window_intervals)

# Save to CSV
if not shutdown_df.empty:
    output_file = f'shutdown_cells_{es_mode}.csv'
    shutdown_df.to_csv(output_file, index=False)
    logging.info(f"Saved {output_file}")
    print(f"Cells recommended for shutdown in {es_mode} mode (sample):")
    print(shutdown_df.head(10))
else:
    logging.info(f"No shutdown windows found in {es_mode} mode for 30+ minutes.")
    print(f"No cells meet the shutdown criteria in {es_mode} mode for 30+ minutes.")