import pymongo
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
from functools import reduce
import numpy as np
import random

plt.style.use('ggplot')

# MongoDB URI
mongo_uri = "mongodb+srv://Mercedeh:UBY3JHxZAkbEQUAa@iflab.ouvlq0u.mongodb.net/mydb?retryWrites=true&w=majority"

# Collection names
collections = {
    'a1': 'ooni_web_collection',
    'b1': 'ooni_circumvention_collection',
    'c1': 'shut_collection',
    'd1': 'radar_ip_version_collection',
    'd2': 'radar_tls_version_collection'
}

# Define the date columns for each dataset
date_columns = {
    'a1': 'measurement_start_day',
    'b1': 'measurement_start_day',
    'c1': 'start_date',  # Choose either 'start_date' or 'end_date'
    'd1': 'Serie_0 timestamps',
    'd2': 'Serie_0 timestamps'
}

# Function to read data from MongoDB
def read_mongodb_data(collection_name):
    client = pymongo.MongoClient(mongo_uri)
    db = client['mydb']  # Replace 'mydb' with your database name
    collection = db[collection_name]
    cursor = collection.find({})  # You can add query conditions here if needed
    data = list(cursor)
    client.close()
    df = pd.DataFrame(data)
    return df


def a1_data_prep(collection_name, fq='M'):
    # Read data from MongoDB for the specified collection
    df = read_mongodb_data(collection_name)
    df['cc'] = df.name.split('_')[-1].strip('.csv')
    print(df.head())
    
    # Rename columns to match the expected format
    df.rename(columns={"measurement_start_day": "date", "confirmed_count": "blocked_count"}, inplace=True)
    
    # Convert the 'date' column to datetime format
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d', errors='coerce')
    
    # Filter for only 2022 data
    df = df.loc[(df['date'] >= '2022-01-01') & (df['date'] <= '2022-12-31')]
    
    # Calculate 'a1' column based on your formula
    df['a1'] = (df['blocked_count'] * 7) + (df['anomaly_count'] * 3)
    
    # Set 'date' as the index
    df.set_index('date', inplace=True)
    
    # Group and aggregate the data
    grouper = df.groupby([pd.Grouper(freq=fq), 'cc'])
    result = grouper['a1'].mean().fillna(0).reset_index()
    
    return result


def b1_data_prep(collection_name, fq='M'):
    # Read data from MongoDB for the specified collection
    df = read_mongodb_data(collection_name)
    
    # Rename columns to match the expected format
    df.rename(columns={"measurement_start_day": "date", "confirmed_count": "blocked_count"}, inplace=True)
    
    # Convert the 'date' column to datetime format
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d', errors='coerce')
    
    # Filter for only 2022 data
    df = df.loc[(df['date'] >= '2022-01-01') & (df['date'] <= '2022-12-31')]
    
    # Calculate 'b1' column based on your formula
    df['b1'] = (df['blocked_count'] * 7) + (df['anomaly_count'] * 3)
    
    # Set 'date' as the index
    df.set_index('date', inplace=True)
    
    # Group and aggregate the data
    grouper = df.groupby([pd.Grouper(freq=fq), 'cc'])
    result = grouper['b1'].mean().fillna(0).reset_index()
    
    return result


def c1_data_prep(collection_name, fq='M'):
    # Read data from MongoDB for the specified collection
    df = read_mongodb_data(collection_name)

    # Join with country code data (assuming you have a 'country' column)
    cc_df = pd.read_csv(cc_filepath)
    df = df.join(cc_df[['name', 'alpha-2']].set_index('name'), on='country')
    df.rename(columns={"alpha-2": "cc"}, inplace=True)

    # Convert date columns to datetime format
    df['start_date'] = pd.to_datetime(df['start_date'])
    df['end_date'] = pd.to_datetime(df['end_date'])

    # Filter for only 2022 data
    df = df.loc[(df['start_date'] >= '2022-01-01') & (df['start_date'] <= '2022-12-31')]

    # Where end_date is null, replace NaT with the value of start_date for that row
    df['end_date'].where(df['end_date'].notnull(), df['start_date'], inplace=True)

    # Calculate the duration of shutdowns in days
    df['duration'] = (df['end_date'] - df['start_date']).dt.days

    # Count the same-day shutdown event as a half-day duration
    df['duration'].where(df['end_date'] != df['start_date'], 0.5, inplace=True)

    # Rename columns and select relevant columns
    df.rename(columns={"start_date": "date"}, inplace=True)
    df['c1'] = df['duration']
    df = df[['date', 'cc', 'c1']]

    # Set 'date' as the index
    df.set_index('date', inplace=True)

    # Group and aggregate the data
    grouper = df.groupby([pd.Grouper(freq=fq), 'cc'])
    result = grouper['c1'].sum().fillna(0).reset_index()

    return result


def d_data_prep(proto_type, collection_name, fq='M'):
    # Read data from MongoDB for the specified collection
    df = read_mongodb_data(collection_name)
    
    # Convert the 'Serie_0 timestamps' column to datetime format
    df['date'] = pd.to_datetime(df['Serie_0 timestamps'], format='ISO8601')
    df['date'] = df['date'].dt.tz_convert(None)
   
    # Filter for only 2022 data
    df = df.loc[(df['date'] >= '2022-01-01') & (df['date'] <= '2022-12-31')]
    
    if proto_type == "IP":
        # Rename columns for IP version data
        df.rename(columns={"Serie_0  I Pv4": "ipv4", "Serie_0  I Pv6": "ipv6"}, inplace=True)
        
        # Calculate 'd1' column based on your formula
        df['d1'] = np.where((df['ipv4'] >= df['ipv6']), (df['ipv6'] / df['ipv4'] * 100), (df['ipv4'] / df['ipv6'] * 100))
        
        # Select relevant columns
        df = df[['date', 'cc', 'd1']]
        df.set_index('date', inplace=True)

        # Group and aggregate the data
        grouper = df.groupby([pd.Grouper(freq=fq), 'cc'])
        result = grouper['d1'].mean().fillna(100).reset_index()
        
    else:
        # Rename columns for TLS version data
        df.rename(columns={"Serie_0  T L S 1.3": "tlsv1_3", "Serie_0  T L S 1.2": "tlsv1_2", 
                           "Serie_0  T L S  Q U I C": "tlsvquic", "Serie_0  T L S 1.0": "tlsv1_0", "Serie_0  T L S 1.1": "tlsv1_1"}, inplace=True)
        
        # Calculate 't_old' and 't_new' columns based on your formula
        df['t_old'] = (df['tlsv1_0'] + df['tlsv1_1'] + df['tlsv1_2'])
        df['t_new'] = (df['tlsv1_3'] + df['tlsvquic'])
        df['d2'] = np.where((df['t_old'] >= df['t_new']), (df['t_new'] / df['t_old'] * 100), (df['t_old'] / df['t_new'] * 100))
        
        # Select relevant columns
        df = df[['date', 'cc', 'd2']]
        df.set_index('date', inplace=True)

        # Group and aggregate the data
        grouper = df.groupby([pd.Grouper(freq=fq), 'cc'])
        result = grouper['d2'].mean().fillna(100).reset_index()

    return result

a1_collection_name = collections['a1'] 
b1_collection_name = collections['b1'] 
c1_collection_name = collections['c1'] 
d1_collection_name = collections['d1'] 
d2_collection_name = collections['d2'] 
a1_df = a1_data_prep(a1_collection_name, fq='M')
b1_df = b1_data_prep(b1_collection_name, fq='M')
c1_df = c1_data_prep(c1_collection_name, fq='M')
d1_df = d_data_prep('IP', d1_collection_name, fq='M')
d2_df = d_data_prep('TLS', d2_collection_name, fq='M')



print(a1_df)
# # Define aggregation function
# def agg_df(cc=True, fq='M'):
#     def min_max_scaling(series):
#         return (series - series.min()) / (series.max() - series.min())
    
#     if cc:
#         data_frames = [a1_df, b1_df, c1_df, d1_df, d2_df]
#         agg_df = reduce(lambda left, right: pd.merge(left, right, on=['date', 'cc'], how='left'), data_frames)
#         agg_df[['a1', 'b1', 'c1']] = agg_df[['a1', 'b1', 'c1']].fillna(0)
#         agg_df[['d1', 'd2']] = agg_df[['d1', 'd2']].fillna(100)
#     else:
#         a1_df2 = a1_df.groupby('date').agg({'a1': 'mean'}).reset_index()
#         b1_df2 = b1_df.groupby('date').agg({'b1': 'mean'}).reset_index()
#         c1_df2 = c1_df.groupby('date').agg({'c1': 'sum'}).reset_index()  # Shouldn't this be sum?
#         d1_df2 = d1_df.groupby('date').agg({'d1': 'mean'}).reset_index()
#         d2_df2 = d2_df.groupby('date').agg({'d2': 'mean'}).reset_index()
        
#         data_frames = [a1_df2, b1_df2, c1_df2, d1_df2, d2_df2]
#         agg_df = reduce(lambda left, right: pd.merge(left, right, on=['date'], how='left'), data_frames)
#         agg_df[['a1', 'b1', 'c1']] = agg_df[['a1', 'b1', 'c1']].fillna(0)
#         agg_df[['d1', 'd2']] = agg_df[['d1', 'd2']].fillna(100)
    
#     col = agg_df.select_dtypes("number").columns
#     weights = {'a1': 30, 'b1': 10, 'c1': 40, 'd1': 10, 'd2': 10}
    
#     # Apply weight multiplication calculation
#     agg_df[col] = agg_df[col].mul(weights)
    
#     # Apply min-max scaling (keep values between 0-1)
#     agg_df[col] = min_max_scaling(agg_df[col])
    
#     # Fragmentation index (findex) is the mean of all features (after weighting and min/max calculations have been applied)
#     agg_df["findex"] = agg_df.loc[:, col].mean(axis=1).round(2)
    
#     return agg_df

# # Aggregation
# mon_cc_agg_df = agg_df(cc=True, fq='M')
# mon_agg_df = agg_df(cc=False, fq='M')
# ann_cc_agg_df = agg_df(cc=True, fq='Y')
# ann_agg_df = agg_df(cc=False, fq='Y')

# # Plotting functions

# def plot_timeseries(df):
#     fig = go.Figure(data=[go.Scatter(x=df['date'], y=df['findex'], mode='lines+markers')])
#     fig.update_xaxes(dtick="M1", tickformat="%b\n%Y")
#     fig.show()

# def plot_cc_timeseries(df, cc):
#     cc_df = df[df['cc'].isin(cc)]
#     fig = go.Figure()
#     for code in cc:
#         cc_data = cc_df[cc_df['cc'] == code]
#         fig.add_trace(go.Scatter(x=cc_data['date'], y=cc_data['findex'], mode='lines+markers', name=code))
#     fig.update_xaxes(dtick="M1", tickformat="%b\n%Y")
#     fig.show()

# # Plotting

# fig = go.Figure(data=[go.Table(
#     header=dict(values=['Country Code', 'Fragmentation Index'],
#                 fill_color='paleturquoise',
#                 align='left'),
#     cells=dict(values=ann_cc_agg_df[['cc', 'findex']].transpose().values.tolist(),
#                fill_color='lavender',
#                align='left'))
# ])

# fig.show()


# def create_map_plot(df):
#     cc_df = pd.read_csv(cc_filepath)
#     df = df.join(cc_df[['alpha-2', 'alpha-3', 'name']].set_index('alpha-2'), on='cc')
   
#     fig = px.choropleth(df, locations="alpha-3",
#                     color="findex",  # lifeExp is a column of gapminder
#                     hover_name="name",  # column to add to hover information
#                     color_continuous_scale=px.colors.sequential.Plasma,
#                     height=500,
#                     width=1000)
#     fig.show()

# def create_radar_plot(df, cc):
#     subjects = ['web_censorship', 'circumvention_tools', 'internet_shutdowns', 'ip_versions', 'tls_versions']
#     cc_df = df[df['cc'] == cc].drop(columns=['date', 'cc', 'findex'])
#     val = cc_df.values.tolist()[0]
#     angles = np.linspace(0, 2*np.pi, len(subjects), endpoint=False)
#     angles = np.concatenate((angles, [angles[0]]))
#     subjects.append(subjects[0])
    
#     fig = plt.figure(figsize=(6, 6))
#     ax = fig.add_subplot(111, polar=True)
#     ax.plot(angles, val, 'o-', color='g', linewidth=1, label=cc)
#     ax.fill(angles, val, alpha=0.25, color='g')
#     ax.set_thetagrids(angles * 180/np.pi, subjects)
    
#     plt.tight_layout()
#     plt.legend()
#     plt.show()

# def create_radar_multiplot(df, cc):
#     colors = "bgrcmykw"
#     color_index = 0
#     d = {}
#     subjects = ['web_censorship', 'circumvention_tools', 'internet_shutdowns', 'ip_versions', 'tls_versions']
#     angles = np.linspace(0, 2*np.pi, len(subjects), endpoint=False)
#     angles = np.concatenate((angles, [angles[0]]))
#     subjects.append(subjects[0])
    
#     fig = plt.figure(figsize=(5, 5))
#     ax = fig.add_subplot(111, polar=True)
#     ax.set_thetagrids(angles * 180/np.pi, subjects)
    
#     for i in cc:
#         random_color = ["#" + ''.join([random.choice('ABCDEF0123456789') for i in range(6)])]
#         cc_df = df[df['cc'] == i].drop(columns=['date', 'cc', 'findex'])
#         d[f"val_{i}"] = cc_df.values.tolist()[0]
#         d[f"val_{i}"].append(d[f"val_{i}"][0])
#         ax.plot(angles, d[f"val_{i}"], 'o-', color=colors[color_index], linewidth=1, label=i)
#         ax.fill(angles, d[f"val_{i}"], alpha=0.25, color=colors[color_index])
#         color_index += 1
    
#     plt.tight_layout()
#     plt.legend()
#     plt.show()

# # Plotting

# fig = go.Figure(data=[go.Table(
#     header=dict(values=['Country Code', 'Fragmentation Index'],
#                 fill_color='paleturquoise',
#                 align='left'),
#     cells=dict(values=ann_cc_agg_df[['cc', 'findex']].transpose().values.tolist(),
#                fill_color='lavender',
#                align='left'))
# ])

# fig.show()

# def create_frag_spec(df):
#     a = [1,2,5,6,9,11,15,17,18]

#     plt.hlines(1,1,20)  # Draw a horizontal line
#     plt.xlim(0,21)
#     plt.ylim(0.5,1.5)

#     y = np.ones(np.shape(a))   # Make all y values the same
#     plt.plot(a,y,'|',ms = 40)  # Plot a line at each location specified in a
#     plt.axis('off')
#     plt.show()

# plot_timeseries(mon_agg_df)

# cc = ['US', 'BR', 'IN', 'IQ', 'RW']
# plot_cc_timeseries(mon_cc_agg_df, cc)

# create_map_plot(ann_cc_agg_df)

# cc = ['GB', 'IN']
# create_radar_multiplot(ann_cc_agg_df, cc)

# create_radar_plot(ann_cc_agg_df, 'IQ')
# cc = ['US', 'BR', 'IN', 'IQ', 'RW']
# for i in cc:
#     create_radar_plot(ann_cc_agg_df, i)

