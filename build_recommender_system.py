import pandas as pd
import numpy as np

class Recom_sys:
    def __init__(self, movie_dataset, rating_dataset):
        self.movie_dataset = movie_dataset
        self.rating_dataset = rating_dataset

    def read_data(self):
        movie_df = pd.read_csv(self.movie_dataset, sep='\t')
        rating_df = pd.read_csv(self.rating_dataset, sep='\t')
        
        self.movie_df = movie_df
        self.rating_df = rating_df

    def cleaningData_movie(self, data):
        # 5 data teratas
        print(self.movie_df.head())
        
        # Cek info kolom
        print(self.movie_df.info()) 

        # Cek Null
        print(self.movie_df.isnull().sum())

        # Analisis Kolom dengan data bernilai NULL - 'primaryTitle' & 'originalTitle'
        print(self.movie_df.loc[(self.movie_df['primaryTitle'].isnull()) | (self.movie_df['originalTitle'].isnull())])

        # Membuang Data dengan Nilai NULL - 'primaryTitle' & 'originalTitle'
        # mengupdate movie_df dengan membuang data-data bernilai NULL
        self.movie_df = self.movie_df.loc[(self.movie_df['primaryTitle'].notnull()) & (self.movie_df['originalTitle'].notnull())]

        # menampilkan jumlah data setelah data dengan nilai NULL dibuang
        print('jumlah data setelah data dengan nilai NULL dibuang = ', len(self.movie_df))

        # Analisis Kolom dengan data bernilai NULL - 'genres'
        print(self.movie_df.loc[self.movie_df['genres'].isnull()])

        # Membuang Data dengan Nilai NULL - 'genres'
        # mengupdate movie_df dengan membuang data-data bernilai NULL
        self.movie_df = self.movie_df.loc[self.movie_df['genres'].notnull()]

        # menampilkan jumlah data setelah data dengan nilai NULL dibuang
        print('jumlah data setelah data dengan nilai NULL dibuang = ', len(self.movie_df))

        # Mengubah Nilai '\N'
        #mengubah nilai '\\N' pada startYear menjadi np.nan dan cast kolomnya menjadi float64
        self.movie_df['startYear'] = self.movie_df['startYear'].replace('\\N', np.nan)
        self.movie_df['startYear'] = self.movie_df['startYear'].astype('float64')
        print(self.movie_df['startYear'].unique()[:5])

        #mengubah nilai '\\N' pada endYear menjadi np.nan dan cast kolomnya menjadi float64
        self.movie_df['endYear'] = self.movie_df['endYear'].replace('\\N', np.nan)
        self.movie_df['endYear'] = self.movie_df['endYear'].astype('float64')
        print(self.movie_df['endYear'].unique()[:5])

        #mengubah nilai '\\N' pada runtimeMinutes menjadi np.nan dan cast kolomnya menjadi float64
        self.movie_df['runtimeMinutes'] = self.movie_df['runtimeMinutes'].replace('\\N', np.nan)
        self.movie_df['runtimeMinutes'] = self.movie_df['runtimeMinutes'].astype('float64')
        print(self.movie_df['runtimeMinutes'].unique()[:5])

        # Mengubah nilai genres menjadi list
        def transform_to_list(x):
            if ',' in x: 
            # ubah menjadi list apabila ada data pada kolom genre
                return x.split(',')
            else: 
            # jika tidak ada data, ubah menjadi list kosong
                return []

        self.movie_df['genres'] = self.movie_df['genres'].apply(lambda x: transform_to_list(x))

        # menampilkan 5 data teratas
        print(self.movie_df['genres'].head())

        return self.movie_df

    def cleaningData_rating(self, data):
        # 5 data teratas
        print(self.rating_df.head()) 

        # Cek info kolom
        print(self.rating_df.info()) 

        # Cek Null
        print(self.rating_df.isnull().sum())

        return self.rating_df

    def join_data(self, cleanData_movie, cleanData_Rating): # Joining table movie and table ratings
        # melakukan join pada kedua dataframe
        movie_rating_df = pd.merge(self.movie_df, self.rating_df, on='tconst', how='inner')

        # menampilkan 5 data teratas
        print(movie_rating_df.head())

        # menampilkan tipe data dari tiap kolom
        print(movie_rating_df.info())

        # Memperkecil ukuran Table
        movie_rating_df = movie_rating_df.dropna(subset=['startYear','runtimeMinutes'])

        # Untuk memastikan tidak ada lagi nilai NULL
        print('Data setelah membuang nilai null =')
        print(movie_rating_df.info())

        # Save data to csv
        movie_rating_df.to_csv('data\movie_rating.csv')

        self.movie_rating_df = movie_rating_df

    def mean_averageRating(self):
        final_data = pd.read_csv('data\movie_rating.csv')
        print(final_data['averageRating'].mean())

    def weighted_formula(self):
        movie_rating_df = pd.read_csv('data\movie_rating.csv')
        def imdb_weighted_rating(df, var=0.8):
            v = df['numVotes']
            R = df['averageRating']
            C = df['averageRating'].mean()
            m = df['numVotes'].quantile(var)
            df['score'] = (v/(m+v))*R + (m/(m+v))*C # rumus IMDb 
            return df['score']
            
        imdb_weighted_rating(movie_rating_df)

        # melakukan pengecekan dataframe
        print(movie_rating_df.head())

        def simple_recommender(df, top=50):
            df = df.loc[df['numVotes'] >= df['numVotes'].quantile(0.8)]
            df = df.sort_values(by='score',ascending=False) #urutkan dari nilai tertinggi ke terendah
            
            # mengambil 50 teratas
            df = df[:top]
            return df
            
        # mengambil 25 data teratas     
        print(simple_recommender(movie_rating_df, top=25))

        df = movie_rating_df.copy()
        
        def user_prefer_recommender(df, ask_adult, ask_start_year, ask_genre, top=100):
            #ask_adult = yes/no
            if ask_adult.lower() == 'yes':
                df = df.loc[df['isAdult'] == 1]
            elif ask_adult.lower() == 'no':
                df = df.loc[df['isAdult'] == 0]

            #ask_start_year = numeric
            df = df.loc[df['startYear'] >= int(ask_start_year)]

            #ask_genre = 'all' atau yang lain
            if ask_genre.lower() == 'all':
                df = df
            else:
                def filter_genre(x):
                    if ask_genre.lower() in str(x).lower():
                        return True
                    else:
                        return False
                df = df.loc[df['genres'].apply(lambda x: filter_genre(x))]

            df = df.loc[df['numVotes'] >= df['numVotes'].quantile(0.8)] #Mengambil film dengan m yang lebih besar dibanding numVotes
            df = df.sort_values(by='score', ascending=False)
            
            #jika kamu hanya ingin mengambil 100 teratas
            df = df[:top]
            return df

        print(user_prefer_recommender(df,
                            ask_adult = 'no',
                                ask_start_year = 2000,
                            ask_genre = 'drama'
                            ))       


movie_dataset = 'https://dqlab-dataset.s3-ap-southeast-1.amazonaws.com/title.basics.tsv'
rating_dataset = 'https://dqlab-dataset.s3-ap-southeast-1.amazonaws.com/title.ratings.tsv'

app = Recom_sys(movie_dataset, rating_dataset)
data = app.read_data()
cleanData_Movie = app.cleaningData_movie(data)
cleanData_Rating = app.cleaningData_rating(data)
app.join_data(cleanData_Movie, cleanData_Rating)
app.mean_averageRating()
app.weighted_formula()