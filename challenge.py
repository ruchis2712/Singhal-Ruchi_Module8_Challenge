#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Import dependencies
import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
import psycopg2

from config import db_password

import time


# In[ ]:


file_dir = "data_files"


# In[ ]:


def data_transform(wiki_data, kaggle_data, ratings_data):
    
    # assign parameters
    wiki_movies_raw=wiki_data
    kaggle_metadata=kaggle_data
    ratings=ratings_data
    
    # create dataframe for wiki movies
    wiki_movies=pd.DataFrame(wiki_movies_raw)
    
    # create dataframe with only rows having a director and no 'no of episodes' i.e. only movies
    try:
        wiki_movies2= (movie for movie in wiki_movies_raw
                       if ('Director' in movie or 'Directed by' in movie) 
                       and 'imdb_link' in movie
                       and 'No. of episodes' not in movie)
    except (KeyError):
        print ("KeyError -- One of the columns expected does not exist the data")
    
    # DEFINE FUNCTION to Clean Movie Data
    
    def clean_movie(movie):
        movie = dict(movie) #create a non-destructive copy
        alt_titles = {}
        # combine alternate titles into one list
        for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune-Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:
            if key in movie:
                alt_titles[key] = movie[key]
                movie.pop(key)
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles
        
        # merge column names
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)
        
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')
        
        return movie
    
        
    # Call clean movie function for every line item in the data frame
    clean_movies = [clean_movie(movie) for movie in wiki_movies2]
    
    # recreate dataframe from the clean movie list
    wiki_movies_df = pd.DataFrame(clean_movies)
    
    # Extract imdb ID
    wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
    # Drop duplicate IDs
    wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
    
    # Keep onlu columns with atleast 90% values. Remove columns which are less than 90% populated
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
    

    # Function to convert Box Office values to Float $
    def parse_dollars(s):
        # if s is not a string, return NaN
        if type(s) != str:
            return np.nan
    
        # if input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):
            # remove dollar sign and " million"
            s = re.sub('\$|\s|[a-zA-Z]','', s)
        
            # convert to float and multiply by a million
            value = float(s) * 10**6

            # return value
            return value

        # if input is of the form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
            
            # remove dollar sign and " billion"
            s = re.sub('\$|\s|[a-zA-Z]','', s)
        
            # convert to float and multiply by a billion
            value = float(s) * 10**9

            # return value
            return value

        # if input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

            # remove dollar sign and commas
            s = re.sub('\$|,','', s)
        
            # convert to float
            value = float(s)

            # return value
            return value

        # otherwise, return NaN
        else:
            return np.nan
 
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
    
    try:
        
        # Drop rows where no Box Office data available i.e. NA
        box_office = wiki_movies_df['Box office'].dropna()
    
        # Clean and convert Box Office numbers 
        box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
        box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
        matches_form_one = box_office.str.contains(form_one, flags=re.IGNORECASE)
        matches_form_two = box_office.str.contains(form_two, flags=re.IGNORECASE)
          
        # Call function to convert into $
        wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    
        # Drop old column
        wiki_movies_df.drop('Box office', axis=1, inplace=True)
    
    except (KeyError):
        print ("Column Box office not found")
        pass
    
    try:
           
        # Clean Budget Column and convert to $
        budget = wiki_movies_df['Budget'].dropna()
        budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
        budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
        budget = budget.str.replace(r'\[\d+\]\s*', '')
        matches_one = budget.str.contains(form_one, flags=re.IGNORECASE)
        matches_two = budget.str.contains(form_two, flags=re.IGNORECASE)
    
        wiki_movies_df['budget_clean'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    
        # Drop Old Column
        wiki_movies_df.drop('Budget', axis=1, inplace=True)
    
    except (KeyError):
        print ("Column Budget not found")
        pass
    
    
    try:
        
        # make a variable that holds the non-null values of Release date in the DataFrame, converting lists to strings
        release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    
        date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
        date_form_two = r'\d{4}.[01]\d.[123]\d'
        date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
        date_form_four = r'\d{4}'

        wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
    
        # make a variable that holds the non-null values of Running Time in the DataFrame, converting lists to strings:
        running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    
        running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    
        running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
    
        wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
    
        wiki_movies_df.drop('Running time', axis=1, inplace=True)
    
        #  following code will keep rows where the adult column is False, and then drop the adult column
        kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')

        # code creates the Boolean column we want. We just need to assign it back to video:
        kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
    
        # convert to numeric
        kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
        kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
        kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
    
        # convert to datetime
        kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
    
        ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
        
    except (KeyError, TypeError, NameError) as err:
        print (err)
        pass
    
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
    
    movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)
    
    try:
        ## Drop columns not needed
        movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)
    except (KeyError):
        print ("A column being dropped does not exist")
        pass
    
    
    ## DEFINE FUNCTION to fill in missing Kaggle Data
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(
            lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis=1)
        df.drop(columns=wiki_column, inplace=True)
        
    
    # Call function for missing values

    try:
        fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    except (KeyError):
        pass

    try:
        fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    except (KeyError):
        pass

    try:
        fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
    except (KeyError):
        pass


    # Convert lists to tuples
    for col in movies_df.columns:
        lists_to_tuples = lambda x: tuple(x) if type(x) == list else x
        value_counts = movies_df[col].apply(lists_to_tuples).value_counts(dropna=False)
        num_values = len(value_counts)
    
    ### Turn off SettingWithCopyWarning ##############
    pd.options.mode.chained_assignment = None

    try:
        # Organize Columns    
        movies_df = movies_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                               'runtime','budget','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                               'genres','original_language','overview','spoken_languages','Country',
                               'production_companies','production_countries','Distributor',
                               'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                              ]]
    
        # Rename Columns
        movies_df.rename({'id':'kaggle_id',
                          'title_kaggle':'title',
                          'url':'wikipedia_url',
                          'budget':'budget',
                          'release_date_kaggle':'release_date',
                          'Country':'country',
                          'Distributor':'distributor',
                          'Producer(s)':'producers',
                          'Director':'director',
                          'Starring':'starring',
                          'Cinematography':'cinematography',
                          'Editor(s)':'editors',
                          'Writer(s)':'writers',
                          'Composer(s)':'composers',
                          'Based on':'based_on'
                         }, axis='columns', inplace=True)
        
    except:
        print ("KeyError: column does not exist")
        pass
    
    # Group nad Pivot Ratings
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count()                 .rename({'userId':'count'}, axis=1)                 .pivot(index='movieId',columns='rating', values='count')
    
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)

    # LOAD DATA INTO SQL DATABASE TABLES

    try:
        # create string with embedded sql password
        db_string = f"mysql://test:{db_password}@127.0.0.1:5432/movie_data"

        # create engine
        engine = create_engine(db_string)

        movies_df.to_sql(name='movies', con=engine)
  
        rows_imported = 0

        for data in pd.read_csv(f'{file_dir}/ratings.csv', chunksize=1000000):
            print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
            data.to_sql(name='ratings', con=engine, if_exists='append')
            rows_imported += len(data)
        
    except psycopg2.Error as error:
        print (error)
    
    


    return movies_with_ratings_df
    


# In[ ]:


with open(f'{file_dir}/wikipedia-movies.json', mode='r') as file:
    wiki_movies_raw = json.load(file)
    
kaggle_metadata = pd.read_csv(f'{file_dir}/movies_metadata.csv', low_memory=False)

ratings = pd.read_csv(f'{file_dir}/ratings.csv')


# In[ ]:


movies_with_ratings=data_transform(wiki_movies_raw,kaggle_metadata,ratings)

