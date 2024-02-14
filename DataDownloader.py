import requests
import pandas as pd
from pathlib import Path

# Define the list of countries to exclude
excluded_countries = [
    'VG', 'KN', 'ER', 'PW', 'TC', 'FM', 'SX', 'PN', 'DM', 'BL', 'GS', 'MS', 'AI', 'VU', 'TK', 'CX', 'GI', 'NF', 'WF', 'AQ', 'NR',
    'AX', 'SH', 'KP', 'EH', 'GW', 'UM', 'VA', 'PM', 'BV', 'GQ', 'MH', 'WS', 'ZA', 'SM', 'AS', 'FK', 'TF', 'NU', 'KI', 'VI', 'IO',
    'SJ', 'BM', 'HM', 'TO', 'TV', 'MF', 'CK', 'MP', 'NR', 'AX'
]

# Function to load the standard list of country codes
def load_standard_country_codes(cc_filepath):
    cc_df = pd.read_csv(cc_filepath)
    return cc_df['alpha-2'].to_list()

# Function to remove excluded countries and 'nan'
def filter_countries(standard_cc_list, excluded_list):
    filtered_cc = list(set(standard_cc_list).difference(excluded_list))
    filtered_cc.remove('nan')
    return filtered_cc

# Function to download datasets from Cloudflare Radar
def download_radar_dataset(proto, cc):
    url = f"https://api.cloudflare.com/client/v4/radar/http/timeseries/{proto}?dateStart=2022-01-01&dateEnd=2022-12-31&location={cc}&format=csv"
    headers = {
        'X-Auth-Email': 'edsland@gmail.com',
        'X-Auth-Key': '9e244d85618b64e8e9024105880368e0c3f9c',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        filename = f'{proto}_2022_{cc}.csv'
        filepath = f'./data_source/radar/{proto}/'
        with open(f"{filepath}{filename}", "w") as f:
            f.write(response.text)
    except requests.exceptions.HTTPError as errh:
        print ("HTTP Error:", errh)

# Function to download OONI Web Connectivity datasets
def download_ooni_web_connectivity(cc):
    url = f"https://api.ooni.io/api/v1/aggregation?probe_cc={cc}&test_name=web_connectivity&since=2022-01-01&until=2022-12-31&time_grain=day&axis_x=category_code&axis_y=measurement_start_day&format=CSV&download=true"

    try:
        response = requests.get(url)
        response.raise_for_status()
        filename = f'ooni_agg_wc_2022_{cc}.csv'
        filepath = f'./data_source/ooni/wc/'
        with open(f"{filepath}{filename}", "w") as f:
            f.write(response.text)
    except requests.exceptions.HTTPError as errh:
        print ("HTTP Error:", errh)

# Function to download OONI Circumvention Tool datasets
def download_ooni_circumvention_tools(cc):
    url = f"https://api.ooni.io/api/v1/aggregation?probe_cc={cc}&test_name=torsf,tor,stunreachability,psiphon,riseupvpn&since=2022-01-01&until=2022-12-31&time_grain=day&axis_x=test_name&axis_y=measurement_start_day&format=CSV&download=true"

    try:
        response = requests.get(url)
        response.raise_for_status()
        filename = f'ooni_agg_cir_2022_{cc}.csv'
        filepath = f'./data_source/ooni/cir/'
        with open(f"{filepath}{filename}", "w") as f:
            f.write(response.text)
    except requests.exceptions.HTTPError as errh:
        print ("HTTP Error:", errh)

# Main function for downloading datasets
def main():
    # Define the paths to country code files
    cc_filepath = "/data_source/cc_alpha2_3.csv"

    # Load standard country codes and filter excluded countries
    standard_cc_list = load_standard_country_codes(cc_filepath)
    filtered_cc = filter_countries(standard_cc_list, excluded_countries)

    # Download protocol version dataset for a specific country (e.g., 'BR')
    proto = 'tls_version'
    download_radar_dataset(proto, 'BR')

    # Download IP version dataset for all filtered countries
    proto = 'ip_version'
    for cc in filtered_cc:
        download_radar_dataset(proto, cc)

    # Download OONI Web Connectivity dataset for a specific country (e.g., 'UG')
    download_ooni_web_connectivity('UG')

    # Download OONI Web Connectivity dataset for all filtered countries
    for cc in filtered_cc:
        download_ooni_web_connectivity(cc)

    # Download OONI Circumvention Tool dataset for a specific country (e.g., 'SD')
    download_ooni_circumvention_tools('SD')

    # Download OONI Circumvention Tool dataset for all filtered countries
    for cc in filtered_cc:
        download_ooni_circumvention_tools(cc)

if __name__ == "__main__":
    main()
