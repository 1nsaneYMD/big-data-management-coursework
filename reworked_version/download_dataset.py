import os
import shutil
import kagglehub

if __name__ == '__main__':
    path = kagglehub.dataset_download("asaniczka/top-spotify-songs-in-73-countries-daily-updated")

    for filename in os.listdir(path):
        full_file_name = os.path.join(path, filename)
        if os.path.isfile(full_file_name):
            print(os.getcwd())
            shutil.copy(full_file_name, f'{os.getcwd()}/data')
