import pandas as pd
import numpy as np
from scipy.stats import percentileofscore

PROTOTYPES_NAMES = ['Todd Gurley', 'Brandin Cooks', 'Cooper Kupp', 
              'Robert Woods', 'Sammy Watkins', 'Matt Breida', 'Dante Pettis', 'Marquise Goodwin', 'George Kittle']


def clean_position(df):
	df = df.dropna(subset = ['Pos'])
	df['Pos'] = df.Pos.apply(lambda x : str(x).upper())
	df = df[(df['Pos'] == 'RB') | (df['Pos'] == 'WR') | (df['Pos'] == 'TE')]
	return df.reset_index(drop = True)

def drop_unnecessary_columns_rosters(df):
	unwanted_columns = ['No.', 'BirthDate', 'Yrs', 'AV', 'Salary']
	df = df.drop(unwanted_columns, axis = 1)
	return df

def clean_player_column(df):
	player_list = df['Player']
	player_list = [x.replace('*', '').replace('+', '') for x in player_list]
	player_list = [x.split('\\', 1)[0] for x in player_list]
	df['Player'] = player_list
	df = df[~(df.duplicated(subset = ['Player'], keep = 'first'))]
	return df

def fix_column_names(df):
	rename_dict = { 'Att' : 'Rush Att',
					'Yds' : 'Rush Yds',
					'TD' : 'Rush TD',
					'Lng' : 'Rush Lng',
					'Y/A' : 'Rush Y/A',
					'Y/G' : 'Rush Y/G',
					'Yds.1' : 'Receiving Yds',
					'TD.1' : 'Receiving TD',
					'Lng.1' : 'Receiving Lng',
					'Y/G.1' : 'Receiving Y/G',
					'A/G' : 'Rush A/G',
					'Y/R' : 'Receiving Y/R'
					}
	df = df.rename(rename_dict, axis = 1)
	return df

def merge_stats_cols(dfa, dfb):
	to_drop = ['No.', 'Age', 'Pos', 'Rush Lng', 
				'Rush Y/A', 'Rush Y/G', 'Rush A/G', 
				'Receiving Y/R', 'Receiving Lng', 'R/G', 
				'Receiving Y/G', 'Fmb', 'Y/Tch', 'YScm', 'RRTD',
				'Ctch%']
	df1 = dfa.copy()
	df2 = dfb.copy()
	df1 = df1.drop(to_drop, axis = 1)
	df2 = df2.drop(to_drop, axis = 1)
	merged = df1.merge(df2, on = 'Player', how = 'outer', suffixes = (' 2017', ' 2018'))
	merged = merged.fillna(0)
	for i in range(1, 11):
		merged[merged.columns[i][0 : -5]] = merged[merged.columns[i]] + merged[merged.columns[i + 10]]
	cols_to_keep = [c for c in merged.columns if c[-4:] != '2017' and c[-4:] != '2018']
	merged = merged[cols_to_keep]
	merged['Catch Percentage'] = merged['Rec'] / merged['Tgt']
	return merged

def normalize_stats(df):
	temp = df.copy()
	temp['Touch/G'] = temp['Touch'] / temp['G']
	temp['Rush TD/G'] = temp['Rush TD'] / temp['G']
	temp['Rush Att/G'] = temp['Rush Att'] / temp['G']
	temp['Rush Yds/Att'] = temp['Rush Yds'] / temp['Rush Att']
	temp['Rush Yds/G'] = temp['Rush Yds'] / temp['G']
	temp['Tgt/G'] = temp['Tgt'] / temp['G']
	temp['Rec/G'] = temp['Rec'] / temp['G']
	temp['Receiving Yds/Rec'] = temp['Receiving Yds'] / temp['Rec']
	temp['Receiving Yds/G'] = temp['Receiving Yds'] / temp['G']
	temp['Receiving TD/G'] = temp['Receiving TD'] / temp['G']
	to_drop = ['Rush Att', 'Rush Yds', 'Rush TD', 'Tgt', 'Rec', 'Receiving Yds', 'Receiving TD', 'Touch']
	temp = temp.drop(to_drop, axis = 1)
	return temp

def merge_in_stats(dfstats, dfplayers):
	dfplayers = dfplayers.drop(['G', 'GS'], axis = 1)
	return dfplayers.merge(dfstats, on = 'Player', how = 'left')

def clean_receiving_data(d):
	df = d.copy()
	df = df.dropna(subset = ['Pos'])

def position_map_receiving(d):
	df = d.copy()
	position_dict = {'TE/WR' : 'TE',
					'RB/WR' : 'RB',
					'FB/WR' : 'FB',
					'LCB/WR' : 'WR',
					'T/TE' : 'T',
					'RB/TE' : 'FB',
					'CB/RCB' : 'CB',
					'QB/RB' : 'RB',
					'FB/RB/WR' : 'FB',
					'FB/RB/TE' : 'FB',
					'WR' : 'WR',
					'TE' : 'TE',
					'RB' : 'RB',
					'FB' : 'FB',
					'QB' : 'QB',
					'T' : 'T',
					'C' : 'C',
					'DT/LDT/RDT' : 'DT'
					}
	df['Pos'] = df['Pos'].map(position_dict)
	positions_wanted = ['TE', 'RB', 'WR']
	df = df[df['Pos'].isin(positions_wanted)]
	return df


def normalize_all_receiving(d):
	df = d.copy()
	df['Tgt/G'] = df['Tgt'] / df['G']
	df['Rec/G'] = df['Rec'] / df['G']
	df['Receiving Yds/Rec'] = df['Y/R']
	df['Receiving Yds/G'] = df['Y/G']
	df['Receiving TD/G'] = df['TD'] / df['G']
	df['Catch Percentage'] = df['Rec'] / df['Tgt']
	drop_columns = ['Rk', 'Player', 'Tm', 'Tgt', 'Rec', 'Yds', 'Y/R', 'TD', 'Lng', 'R/G', 'Y/G', 'Fmb', 'Ctch%']
	df = df.drop(drop_columns, axis = 1)
	return df

def receiving_percentiles(d, d2):
	df = d.copy()
	df2 = d2.copy()
	tgt_per_game = df.groupby('Pos').apply(lambda x : x['Tgt/G'].tolist()).to_dict()
	rec_per_game = df.groupby('Pos').apply(lambda x : x['Rec/G'].tolist()).to_dict()
	rec_yds_rec = df.groupby('Pos').apply(lambda x : x['Receiving Yds/Rec'].tolist()).to_dict()
	rec_yds_game = df.groupby('Pos').apply(lambda x : x['Receiving Yds/G'].tolist()).to_dict()
	rec_td_game = df.groupby('Pos').apply(lambda x : x['Receiving TD/G'].tolist()).to_dict()
	catch_perc = df.groupby('Pos').apply(lambda x : x['Catch Percentage'].tolist()).to_dict()
	df2 = df2.set_index('Pos')
	receiving_columns = ['Tgt/G', 'Catch Percentage', 'Rec/G', 'Receiving Yds/Rec', 'Receiving Yds/G', 'Receiving TD/G']
	corresponding_dicts = [tgt_per_game, catch_perc, rec_per_game, rec_yds_rec, rec_yds_game, rec_td_game]
	for i in range(len(receiving_columns)):
		temp_series = df2[receiving_columns[i]]
		df2[receiving_columns[i] + ' Percentile'] = [percentileofscore(corresponding_dicts[i][k], v) for k, v in temp_series.items()]
	df2 = df2.drop(receiving_columns, axis = 1)
	df2 = df2.reset_index()
	return df2


def position_map_rushing(d):
	df = d.copy()
	position_dict = { 'QB/RB' : 'RB',
					'QB' : 'QB',
					'RB' : 'RB',
					'RB/WR' : 'RB',
					'WR' : 'WR',
					'TE/WR' : 'TE',
					'QB/WR' : 'QB',
					'FB/WR' : 'FB',
					'FB' : 'FB',
					'FB/RB/TE' : 'FB',
					'RB/TE' : 'FB',
					'TE' : 'TE',
					'DB' : 'DB',
					'CB' : 'DB',
					'LCB/WR' : 'WR',
					'P' : 'P',
					'FS/SS' : 'DB',
					'DT/LDT' : 'DT',
					'DE' : 'DE',
					'FS' : 'DB',
					'DB/S/SS' : 'DB'
					}
	df['Pos'] = df['Pos'].map(position_dict)
	positions_wanted = ['TE', 'RB', 'WR']
	df = df[df['Pos'].isin(positions_wanted)]
	return df

def normalize_all_rushing(d):
	df = d.copy()
	df['Rush Att/G'] = df['Att'] / df['G']
	df['Rush TD/G'] = df['TD'] / df['G']
	df['Rush Yds/Att'] = df['Yds'] / df['Att']
	df['Rush Yds/G'] = df['Yds'] / df['G']
	drop_columns = ['Rk', 'Player', 'Tm', 'Age', 'Att', 'Yds', 'TD', 'Lng', 'Y/A', 'Y/G', 'Fmb']
	df = df.drop(drop_columns, axis = 1)
	return df


def rushing_percentiles(d, d2):
	df = d.copy()
	df2 = d2.copy()
	att_per_game = df.groupby('Pos').apply(lambda x : x['Rush Att/G'].tolist()).to_dict()
	td_per_game = df.groupby('Pos').apply(lambda x : x['Rush TD/G'].tolist()).to_dict()
	yds_per_att = df.groupby('Pos').apply(lambda x : x['Rush Yds/Att'].tolist()).to_dict()
	yds_per_game = df.groupby('Pos').apply(lambda x : x['Rush Yds/G'].tolist()).to_dict()
	df2 = df2.set_index('Pos')
	rush_columns = ['Rush Att/G', 'Rush TD/G', 'Rush Yds/Att', 'Rush Yds/G']
	corresponding_dicts = [att_per_game, td_per_game, yds_per_att, yds_per_game]
	for i in range(len(rush_columns)):
		temp_series = df2[rush_columns[i]]
		df2[rush_columns[i] + ' Percentile'] = [percentileofscore(corresponding_dicts[i][k], v) for k, v in temp_series.items()]
	df2 = df2.drop(rush_columns, axis = 1)
	df2 = df2.reset_index()
	return df2


def create_prototypes(d, d2):
	df1 = d.copy()
	df2 = d2.copy()
	prototypes = pd.concat([df1, df2]).reset_index(drop = True)
	prototypes = prototypes[prototypes['Player'].isin(PROTOTYPES_NAMES)].reset_index(drop = True)
	prototypes = prototypes.drop(['G', 'GS', 'Pos'], axis = 1)
	return prototypes


def rename_college_stats_rushing(d):
	df = d.copy()
	rename_dict = {'Yds' : 'Rush Yds', 
				   'Avg' : 'Rush Yds/Att',
				   'TD' : 'Rush TD',
				   'Yds.1' : 'Receiving Yds',
				   'Avg.1' : 'Yds/Reception',
				   'TD.1' : 'Receiving TD',
				   }
	df = df.rename(rename_dict, axis = 1)
	extra_columns = ['Plays', 'Yds.2', 'Avg.2', 'TD.2']
	df = df.drop(extra_columns, axis = 1)
	return df

def rename_college_stats_receiving(d):
	df = d.copy()
	rename_dict = {'Yds' : 'Receiving Yds',
				   'Avg' : 'Yds/Reception',
				   'TD' : 'Receiving TD',
				   'Yds.1' : 'Rush Yds', 
				   'Avg.1' : 'Rush Yds/Att',
				   'TD.1' : 'Rush TD'}
	df = df.rename(rename_dict, axis = 1)
	extra_columns = ['Plays', 'Yds.2', 'Avg.2', 'TD.2']
	df = df.drop(extra_columns, axis = 1)
	return df

def rename_kupp(d):
	df = d.copy()
	rename_dict = {'Yds' : 'Rush Yds',
				   'Rush' : 'Att',
				   'Yd/Rush' : 'Rush Yds/Att',
				   'TDs' : 'Rush TD',
				   'Yds.1' : 'Receiving Yds',
				   'Yd/Rec' : 'Yds/Reception',
				   'TDs.1' : 'Receiving TD'
				  }
	df = df.rename(rename_dict, axis = 1)
	return df
















