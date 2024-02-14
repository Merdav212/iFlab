import pandas as pd
import matplotlib.pyplot as plt

excluded_list = ['VG', 'KN', 'ER', 'PW', 'TC', 'FM', 'SX', 'PN', 'DM', 'BL', 'GS', 'MS', 'AI', 'VU', 'TK', 'CX', 'GI', 'NF', 'WF', 'AQ', 'NR',
              'AX', 'SH', 'KP', 'EH', 'GW', 'UM', 'VA', 'PM', 'BV', 'GQ', 'MH', 'WS', 'ZA', 'SM', 'AS', 'FK', 'TF', 'NU', 'KI', 'VI', 'IO',
               'SJ', 'BM', 'HM', 'TO', 'TV', 'MF','CK', 'MP','NR', 'AX']

# Function to load the standard list of country codes
def load_standard_country_codes(cc_filepath):
    cc_df = pd.read_csv(cc_filepath)
    return cc_df['alpha-2'].to_list()

# Function to remove excluded countries and 'nan'
def filter_countries(standard_cc_list, excluded_list):
    filtered_cc = list(set(standard_cc_list).difference(excluded_list))
    if 'nan' in filtered_cc:
        filtered_cc.remove('nan')
    return filtered_cc

# Main function for downloading datasets
def main():
    # Define the paths to country code files
    cc_filepath = "/home/lunet/wsmj3/fraganal/data_source/cc_alpha2_3.csv"

    # Load standard country codes and filter excluded countries
    standard_cc_list = load_standard_country_codes(cc_filepath)
    filtered_cc = filter_countries(standard_cc_list, excluded_list)
    dfs_by_country = []
    df_all_countries = pd.DataFrame()

    for cc in filtered_cc:
        file_path = f'/home/lunet/wsmj3/fraganal/data_source/data_source/ooni/wc/ooni_agg_wc_2022_{cc}.csv'
        try:
            df_country = pd.read_csv(file_path)
            # Check if 'measurement_start_day' column exists before using it as a date column
            if 'measurement_start_day' in df_country.columns:
                df_country['cc'] = cc
                dfs_by_country.append(df_country)
            else:
                print(f"Skipping {cc} due to missing 'measurement_start_day' column.")
            print(df_country.head())
    #         # 'cc' column to differentiate countries
            # Append the DataFrame to the list
            dfs_by_country.append(df_country)
        except FileNotFoundError:
            print(f"File not found for {cc}. Skipping...")
            pass

    # Concatenate all DataFrames into a single DataFrame
    df_all_countries = pd.concat(dfs_by_country, ignore_index=True)

    print(df_all_countries.head())
    
    # Group by country and date, sum anomaly count, and plot
    df_anomaly_by_country = df_all_countries.groupby(['cc', 'measurement_start_day'])['anomaly_count'].sum().reset_index()

    # Example: Plot anomaly count over time for each country
# Example: Plot anomaly count over time for each country
    for country, data in df_anomaly_by_country.groupby('cc'):
        plt.plot(data['measurement_start_day'].values, data['anomaly_count'].values, label=country)


    plt.xlabel('Date')
    plt.ylabel('Anomaly Count')
    plt.title('Anomaly Count by Country Over Time')
    plt.legend()
    plt.show()

if __name__ == "__main__":
    main()
