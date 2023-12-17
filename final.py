import pandas as pd
import os
import glob
import numpy as np

def get_excel_params(excel_file):
    # Split the file name using '_'
    file_name = os.path.basename(excel_file)
    parts = file_name.split('_')

    if '.' in parts[-1]:
        parts[-1] = parts[-1].split('.')[0]

    # Split the first part '1B' into two characters
    if len(parts[0]) == 2:
        first_part_splitted = [parts[0][:1], parts[0][1:]]
        # Append each character separately at 0 and 1 positions of the list
        final_splitted_texts_list = [first_part_splitted[0], first_part_splitted[1]]
    else:
        final_splitted_texts_list = [parts[0]]

    # Extend the list with the rest of the splitted parts
    final_splitted_texts_list.extend(parts[1:])

    # Print or use the list of splitted texts as needed
    return final_splitted_texts_list

def process_hex(hex_string):
    hex_bytes = bytes.fromhex(hex_string)
    byte_array = [format(byte, '02x') for byte in hex_bytes]
    last_n_items = byte_array[-9:-1]
    tuples_list = [(int(last_n_items[i], 16), int(last_n_items[i + 1], 16)) for i in range(0, len(last_n_items), 2)]
    sensor_data_list = [combined - 0x10000 if combined & 0x8000 else combined for combined in
                        [(highIndex << 8) | lowIndex for highIndex, lowIndex in tuples_list]]
    return sensor_data_list

def process_file(input_file):
    # Read your CSV file into a DataFrame
    df = pd.read_csv(input_file)

    
    # Apply the process_hex function to the 'Data' column and create a new column 'sensor_data'
    df['sensor_data'] = df['Data'].apply(process_hex)

    # Assuming 'splitted_texts_list' is the list of values to add as new columns
    splitted_texts_list = get_excel_params(input_file)

    # Repeat 'splitted_texts_list' for all rows in 'sensor_data_list'
    num_rows = len(df)
    repeated_texts_list = np.tile(splitted_texts_list, num_rows)

    name_list = ['customer_id', 'gender', 'height', 'weight', 'chair_height', 'id-tbc', '']

    # Add the repeated list as new columns to the DataFrame
    for i, col_name in enumerate(range(len(splitted_texts_list))):
        df[name_list[i]] = repeated_texts_list[i::len(splitted_texts_list)]

    # Split 'sensor_data' column and add each value to a different column
    df[['sensor_data_1', 'sensor_data_2', 'sensor_data_3', 'sensor_data_4']] = pd.DataFrame(df['sensor_data'].tolist(), index=df.index)

    df = df.drop(columns=['No.', 'Time', 'Source', 'Destination', 'Protocol', 'Length', 'Data', 'Info', 'Data','sensor_data'])

    return df

# Specify the input and output directories
input_directory = r'C:\Users\Chathura\OneDrive - Veracity Group (Pvt) Ltd\Documents\Projects\sgi_data\Input'
output_directory = r'C:\Users\Chathura\OneDrive - Veracity Group (Pvt) Ltd\Documents\Projects\sgi_data\Output'
output_file = r'C:\Users\Chathura\OneDrive - Veracity Group (Pvt) Ltd\Documents\Projects\sgi_data\Output\File.csv'

# Get a list of CSV files in the input directory
csv_files = glob.glob(os.path.join(input_directory, '*.csv'))

# Process each file and concatenate DataFrames
dfs = [process_file(csv_file) for csv_file in csv_files]

combined_df = pd.concat(dfs, ignore_index=True)

# Save the combined DataFrame to a CSV file
combined_df.to_csv(output_file, index=False)
