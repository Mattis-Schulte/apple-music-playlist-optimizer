# Apple Music Playlist Optimizer
**Please note:** Example files of the ```identified_songs``` and ```calculated_path``` files are included in the repository, those might be overwritten when running the scripts. An example of a playlist created by this project can be found [here](https://music.apple.com/de/playlist/panoptic-cumquats/pl.u-d2b08D2FMblGY25).
## Scope of this project
This project aims to curate a personalized playlist by analyzing the user's Apple Music listing history, which is available for download [here](https://privacy.apple.com/account). A key feature of this project is employing Graph Theory to discern possible relationships between songs, which can then be used to order the songs more optimally and personally. This project is written in Python and uses the [Apple Music API](https://developer.apple.com/documentation/applemusicapi) to upload the compiled playlist directly to the user's Apple Music account.

## Prerequisites
- Login in into [music.apple.com](https://music.apple.com/) with your Apple ID, and obtain ```MEDIA_USER_TOKEN``` and ```AUTH_TOKEN``` from the network tab in the developer tools. Place your tokens in the corresponding fields in ```.env``` file.
- Download your Apple Music data from [here](https://privacy.apple.com/account). Make sure you have both the ```Apple Music Play Activity``` and ```Apple Music - Play History Daily Tracks``` files. 

## Cross-referencing
This step is necessary as ```Apple Music Play Activity``` does not contain the track identifiers, which are necessary to upload the playlist to Apple Music. The script ```crossreference.py``` cross-references the ```Apple Music Play Activity``` file with the ```Apple Music - Play History Daily Tracks``` file, which does include the track identifiers but also a for our purposes inadequate timestamp, hence the need for cross-referencing.

Place the path to both files in the corresponding fields at the bottom of the script and run the script (this might take multiple hours based on the size of the files). The output will be a file called ```identified_songs```, which will contain both the track identifiers and the timestamps as well as the play durations and media durations for each stream.

## Calculating the optimal order
To now calculate the optimal order of the songs, run the script ```calculate_optimal_path.py```. This will create a graph of the songs and their relationships to each other, which will then be used to choose the optimal order based on the weights of the edges. The output will be a file called ```calculated_path```, which will contain the songs in their calculated optimal order. 
>Please note that all songs that were not played for a cumulative duration of at least five minutes will be excluded from the playlist. Streams that lasted less than 25 seconds will also be deemed as skipped and therefore are not included in the calculation of the optimal order.

## Uploading the playlist
To upload the playlist to your Apple Music account, run the script ```export_playlist_to_apple_music.py```. This will upload the playlist to your Apple Music account on a song-by-song basis. The script will also automatically try to find an alternative version of the song if the original version is no longer available in the selected Apple Music catalog.
