import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt

class PlotUtilsService:
    @classmethod
    def plot_bar_plot(cls, data, x, y, title, xlabel, ylabel):
        fig = plt.figure(figsize=(12, 6))
        plt.barh(data[x], data[y])
        plt.title(title, fontsize=16)
        plt.xlabel(xlabel, fontsize=12)
        plt.ylabel(ylabel, fontsize=12)
        plt.tight_layout()
        return fig
    
    @classmethod
    def close_figure(cls, figure):
        plt.close(figure)

    @classmethod
    def plot_line_plot(cls, data):
        global_rank_1_songs = data[(data['daily_rank'] == 1) & (data['country'] == 'Global')]
        global_rank_1_songs_sorted = global_rank_1_songs.sort_values(by='snapshot_date')

        dates = global_rank_1_songs_sorted['snapshot_date']
        danceability = global_rank_1_songs_sorted['danceability']
        energy = global_rank_1_songs_sorted['energy']
        valence = global_rank_1_songs_sorted['valence']

        fig = plt.figure(figsize=(14, 8))

        plt.plot(dates, danceability, marker='o', linestyle='-', linewidth=1, label='Danceability', color='b')
        plt.plot(dates, energy, marker='s', linestyle='-', linewidth=1, label='Energy', color='r')
        plt.plot(dates, valence, marker='^', linestyle='-', linewidth=1, label='Valence', color='g')

        plt.title('Changes in Danceability, Energy, and Valence for Global #1 Ranked Song(s) Over Time', fontsize=16)
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Feature Value', fontsize=12)
        plt.xticks(rotation=45)
        plt.legend(title='Feature', loc='upper left', bbox_to_anchor=(1.05, 1))
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.tight_layout()
        
        return fig

    @classmethod
    def plot_line_plot_song_popularity(cls, data):
        song_popularity = data[data['name'] == "All I Want for Christmas Is You"]

        fig = plt.figure(figsize=(14, 6))

        plt.plot(song_popularity['snapshot_date'], song_popularity['popularity'], color='crimson')
        plt.title('Popularity Trend of "All I Want for Christmas Is You"', fontsize=16)
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Popularity', fontsize=12)

        plt.xticks(rotation=45)
        plt.grid(axis='y', linestyle='--', alpha=0.7)

        plt.tight_layout()

        return fig
