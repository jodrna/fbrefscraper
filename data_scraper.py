import requests
from bs4 import BeautifulSoup
import re
from unidecode import unidecode as decode
import pandas as pd
import numpy as np


# import current database of matches to be updated
database = pd.read_csv('~/Documents/football/raw_data/matches.csv', parse_dates=['datetime']).drop(['Unnamed: 0'], axis=1)
# leagues to scrape and date intervals
start_date = '15/02/2021 00:00:00'
end_date = '15/02/2021 23:59:59'
leagues = ['https://fbref.com/en/comps/13/schedule/Ligue-1-Scores-and-Fixtures',
           'https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures',
           'https://fbref.com/en/comps/20/schedule/Bundesliga-Scores-and-Fixtures',
           'https://fbref.com/en/comps/11/schedule/Serie-A-Scores-and-Fixtures',
           'https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures']


# loop through league pages and scrape links for each individual match and some match info, final output dataframe called matches
matches = []
for league in leagues:
    html = BeautifulSoup(requests.get(league).text, 'html.parser')
    data = html.find_all('tbody')[0]
    data = data.find_all('tr')

    for datum in data:
        try:
            url = datum.contents[6].find("a", href=re.compile("")).attrs['href']
            date = datum.contents[2].text
            time = datum.contents[3].text
            season = '2020-2021'
            league = re.split('-[0-9][0-9][0-9][0-9]-', url)[1]
            home = datum.contents[4].text
            away = datum.contents[8].text
            stadium = datum.contents[10].text
            attendance = datum.contents[9].text
            referee = datum.contents[11].text
            matches.append((url, date + ' ' + time, season, league, home, away, stadium, attendance, referee))
        except:
            pass

# turn table into dataframe, clean...name columns, remove cancelled matches, filter out matches not within set timeframe
matches = pd.DataFrame(matches)
for col in matches.columns:
    matches[col] = matches[col].apply(decode)
matches.columns = ['url', 'datetime', 'season', 'league', 'home', 'away', 'stadium', 'attendance', 'referee']
matches = matches.replace('', np.nan)
matches = matches.dropna(subset=['referee'])
matches['datetime'] = pd.to_datetime(matches['datetime'])
matches = matches.loc[(matches['datetime'] >= start_date) & (matches['datetime'] <= end_date)]
matches = matches.sort_values(by='datetime').reset_index(drop=True)


# loop through the matches dataframe, opening the url and scraping data for each, final output match_info and match stats
match_info = []
match_stats = []
for match in matches.iloc[0:, 0]:
    html = BeautifulSoup(requests.get("https://fbref.com/" + match).text, 'html.parser')

    # scrape non stat match info and append to match info table
    home = html.find_all('div', {'class': re.compile('lineup'), 'id': 'a'})[0].contents[1].contents[1].contents[0].contents[0].split(' (')[0]
    home_formation = '(' + html.find_all('div', {'class': re.compile('lineup'), 'id': 'a'})[0].contents[1].contents[1].contents[0].contents[0].split(' (')[1]
    away = html.find_all('div', {'class': re.compile('lineup'), 'id': 'b'})[0].contents[1].contents[1].contents[0].contents[0].split(' (')[0]
    away_formation = '(' + html.find_all('div', {'class': re.compile('lineup'), 'id': 'b'})[0].contents[1].contents[1].contents[0].contents[0].split(' (')[1]
    match_info.append((match, 'Home', home, home_formation, 'Away', away, away_formation))
    match_info.append((match, 'Away', away, away_formation, 'Home', home, home_formation))

    # isolate match stats and prepare to scrape
    all_stats = []
    data = html.find_all('div', {'id': re.compile('div_stats_')})
    for datum in data:
        data = datum.find('tfoot').contents[0].contents
        all_stats.append(data)

    # loop through all stats and scrape home stats
    home_stats = []
    for stats in all_stats[0:5]:
        for stat in stats[6:]:
            home_stat = stat.next_element
            home_stats.append(home_stat)
    home_stats = pd.DataFrame(home_stats)
    home_stats = home_stats.transpose()
    home_stats[119], home_stats[120] = 'Home', match
    try:
        match_stats = pd.concat([match_stats, home_stats], axis=0)
    except:
        match_stats = home_stats

    # loop through all stats and scrape away stats
    away_stats = []
    for stats in all_stats[6:11]:
        for stat in stats[6:]:
            away_stat = stat.next_element
            away_stats.append(away_stat)
    away_stats = pd.DataFrame(away_stats)
    away_stats = away_stats.transpose()
    away_stats[119], away_stats[120] = 'Away', match
    match_stats = pd.concat([match_stats, away_stats], axis=0)


# set tables as dataframes and name columns etc
match_info = pd.DataFrame(match_info)
match_stats = pd.DataFrame(match_stats)
match_info.columns = ['url', 'attack', 'attack_team', 'attack_formation', 'defence', 'defence_team', 'defence_formation']
match_stats.columns = ['goals', 'assists', 'pens_made', 'pens_att', 'shots_total', 'shots_on_target', 'cards_yellow', 'cards_red', 'touches', 'pressures', 'tackles',
                       'interceptions', 'blocks', 'xg', 'npxg', 'xa', 'sca', 'gca', 'passes_completed', 'passes', 'passes_pct', 'progressive_passes', 'carries',
                       'progressive_carries', 'dribbles_completed', 'dribbles', 'passes_completed', 'passes', 'passes_pct', 'passes_total_distance',
                       'passes_progressive_distance', 'passes_completed_short', 'passes_short', 'passes_pct_short', 'passes_completed_medium', 'passes_medium',
                       'passes_pct_medium', 'passes_completed_long', 'passes_long', 'passes_pct_long', 'assists', 'xa', 'assisted_shots', 'passes_into_final_third',
                       'passes_into_penalty_area', 'crosses_into_penalty_area', 'progressive_passes', 'passes', 'passes_live', 'passes_dead', 'passes_free_kicks',
                       'through_balls', 'passes_pressure', 'passes_switches', 'crosses', 'corner_kicks', 'corner_kicks_in', 'corner_kicks_out',
                       'corner_kicks_straight', 'passes_ground', 'passes_low', 'passes_high', 'passes_left_foot', 'passes_right_foot', 'passes_head',
                       'throw_ins', 'passes_other_body', 'passes_completed', 'passes_offsides', 'passes_oob', 'passes_intercepted', 'passes_blocked', 'tackles',
                       'tackles_won', 'tackles_def_3rd', 'tackles_mid_3rd', 'tackles_att_3rd', 'dribble_tackles', 'dribbles_vs', 'dribble_tackles_pct',
                       'dribbled_past', 'pressures', 'pressure_regains', 'pressure_regain_pct', 'pressures_def_3rd', 'pressures_mid_3rd', 'pressures_att_3rd',
                       'blocks', 'blocked_shots', 'blocked_shots_saves', 'blocked_passes', 'interceptions', 'tackles_interceptions', 'clearances', 'errors',
                       'touches', 'touches_def_pen_area', 'touches_def_3rd', 'touches_mid_3rd', 'touches_att_3rd', 'touches_att_pen_area', 'touches_live_ball',
                       'dribbles_completed', 'dribbles', 'dribbles_completed_pct', 'players_dribbled_past', 'nutmegs', 'carries', 'carry_distance',
                       'carry_progressive_distance', 'progressive_carries', 'carries_into_final_third', 'carries_into_penalty_area', 'miscontrols', 'dispossessed',
                       'pass_targets', 'passes_received', 'passes_received_pct', 'progressive_passes_received', 'attack', 'url']
match_stats = match_stats.loc[:, ~match_stats.columns.duplicated()]

# merge the three created dataframes, matches, match_info, and match stats
match = pd.merge(match_info, match_stats, how='left', left_on=['url', 'attack'], right_on=['url', 'attack'])
match = pd.merge(matches, match, how='left', left_on='url', right_on='url')
match = pd.DataFrame(match)

# merge new data with database
database = pd.concat([database, match], axis=0).reset_index(drop=True)
database.to_csv('~/Documents/football/raw_data/matches.csv')
