import logging
import json
import os
import taglib
from smrtypntz.database import db
# import spotipy
import sys


# Init logger
logging.basicConfig(
    stream=sys.stderr,
    level=logging.DEBUG,
    format="%(asctime)-15s %(levelname)s - %(filename)s:%(funcName)s:%(lineno)s\n  %(message)s"
)
logger = logging.getLogger()


# Init database
db.init_tables()

# Load program config, bail if missing.
config = None
with open('config.json') as cfgFile:
    config = json.load(cfgFile)
if config is None:
    exit(-1)

# Make list of files to scan
files = []
try:
    for root, directories, files in os.walk(config.music_dir):
        for name in files:
            if name.endswith('.flac'):
                files.push(os.path.join(root, name))
except OSError or EOFError as e:
    print(e)

# Scan ID3 tags into sqlite
artists = {}
cur_artist = None
albums = {}
cur_album = None
for filename in files:
    song = taglib.File(filename)
    artist = db.Artist(song)
    if artist.needs_split():
        album_artist = artist.split()
        if artist != album_artist:
            pass
    album = db.Album(song)
    track = db.Track(song)
    print(song.tags)
    print(song.length)
    print(song.bitrate)
    print(song.sampleRate)
    print(song.channels)
    song.close()
