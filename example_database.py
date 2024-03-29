from __future__ import annotations

from squeeb.db import make_database_class
from squeeb.model import column, DataType, PrimaryKey, ForeignKey
from squeeb.model.decorators import table
from squeeb.model.index import TableIndex, IndexedColumn
from squeeb.model.models import Model


SmrtyPntz = make_database_class('SmrtyPntz', 'music.db')


@table(db_class=SmrtyPntz)
class Artist(Model):
    id = column(DataType.INTEGER, constraint=PrimaryKey(autoincrement=True, unique=True))
    spotify_id = column(DataType.TEXT)
    name = column(DataType.TEXT)
    spotify_genres = column(DataType.TEXT)
    spotify_popularity = column(DataType.INTEGER)

    artist_names = TableIndex([IndexedColumn(name)], is_unique=True)

    # def needs_split(self) -> bool:
    #     return 'album_artist' in self
    #
    # def split(self) -> Artist:
    #     album_artist = Artist()
    #     album_artist['name'] = self['album_artist']
    #     del self['album_artist']
    #     return album_artist


@table(db_class=SmrtyPntz)
class Album(Model):
    id = column(DataType.INTEGER, constraint=PrimaryKey(autoincrement=True))
    spotify_id = column(DataType.TEXT)
    name = column(DataType.TEXT)
    year = column(DataType.INTEGER)
    genres = column(DataType.TEXT)
    artist_id = column(DataType.INTEGER, constraint=ForeignKey(Artist, Artist.id))
    spotify_genres = column(DataType.TEXT)

    artist_name_year = TableIndex([IndexedColumn(name), IndexedColumn(year), IndexedColumn(artist_id)], is_unique=True)


@table(db_class=SmrtyPntz)
class Track(Model):
    id = column(DataType.INTEGER, constraint=PrimaryKey(autoincrement=True))
    spotify_id = column(DataType.TEXT)
    filepath = column(DataType.TEXT)
    name = column(DataType.TEXT)
    duration = column(DataType.INTEGER)
    artist_id = column(DataType.INTEGER, constraint=ForeignKey(Artist, Artist.id))
    album_artist_id = column(DataType.INTEGER, constraint=ForeignKey(Artist, Artist.id))
    album_id = column(DataType.INTEGER, constraint=ForeignKey(Album, Album.id))
    album_track_number = column(DataType.INTEGER)
    danceability = column(DataType.REAL)
    energy = column(DataType.REAL)
    instrumentalness = column(DataType.REAL)
    key = column(DataType.INTEGER)
    liveness = column(DataType.REAL)
    loudness = column(DataType.REAL)
    mode = column(DataType.INTEGER)
    speechiness = column(DataType.REAL)
    tempo = column(DataType.REAL)
    time_signature = column(DataType.INTEGER)
    valence = column(DataType.REAL)


# @database(filename="smrtypntz.db")
# class _MusicDb(AbstractDbHandler):
#
#     # todo: crud methods for music data
#
#     # def _init_tables(self) -> bool:
#     #     success = True
#     #     if self._table_exists("artists") is False:
#     #         success = success and self._create_table_artists()
#     #     if self._table_exists("albums") is False:
#     #         success = success and self._create_table_albums()
#     #     if self._table_exists("tracks") is False:
#     #         success = success and self._create_table_tracks()
#     #     return success
#
#     def _create_table_artists(self) -> bool:
#         return self._exec_raw_query_no_result('''CREATE TABLE "artists" (
#                 "id" INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
#                 "spotify_id" TEXT,
#                 "name" TEXT,
#                 "spotify_genres" TEXT,
#                 "spotify_popularity" INTEGER
#             )''') and self._exec_raw_query_no_result('''CREATE UNIQUE INDEX "artist_names" ON "artists" ("name");''')
#
#     def _create_table_albums(self) -> bool:
#         return self._exec_raw_query_no_result('''CREATE TABLE "albums" (
#                     "id" INTEGER PRIMARY KEY AUTOINCREMENT,
#                     "spotify_id" TEXT,
#                     "name" TEXT,
#                     "year" INTEGER,
#                     "genres" TEXT,
#                     "artist_id" INTEGER REFERENCES "artists"("id"),
#                     "spotify_genres" TEXT
#                 )''') and self._exec_raw_query_no_result('''CREATE UNIQUE INDEX "artist-name-year"
#                                                   ON "albums" ("name","year","artist_id");''')
#
#     def _create_table_tracks(self) -> bool:
#         return self._exec_raw_query_no_result('''CREATE TABLE "tracks" (
#                 "id" INTEGER PRIMARY KEY AUTOINCREMENT,
#                 "spotify_id" TEXT,
#                 "filepath" TEXT,
#                 "name" TEXT,
#                 "duration" INTEGER,
#                 "artist_id" INTEGER REFERENCES "artists"("id"),
#                 "album_artist_id" INTEGER REFERENCES "artists"("id"),
#                 "album_id" INTEGER REFERENCES "albums"("id"),
#                 "album_track_number" INTEGER,
#                 "danceability" REAL,
#                 "energy" REAL,
#                 "instrumentalness" REAL,
#                 "key" INTEGER,
#                 "liveness" REAL,
#                 "loudness" REAL,
#                 "mode" INTEGER,
#                 "speechiness" REAL,
#                 "tempo" REAL,
#                 "time_signature" INTEGER,
#                 "valence" REAL
#             )''')
#
#     def get_all_artists(self) -> List[Artist]:
#         query = SelectQueryBuilder('artists')
#         rows = self.exec_query_all_results(query)
#         # TODO: Deserialize the results into Artist objects
#
#     def get_all_albums(self) -> List[Album]:
#         query = SelectQueryBuilder('albums')
#         rows = self.exec_query_all_results(query)
#         # TODO: Deserialize the results into Album objects
#
#     def get_all_tracks(self) -> List[Track]:
#         query = SelectQueryBuilder('tracks')
#         rows = self.exec_query_all_results(query)
#         # TODO: Deserialize the results into Track objects
#
#
# # Singleton instance of the db handler
# db = _MusicDb()

# artist = Artist.from_dict({"name": 1})
# album = Album.from_dict({"name": "cool"})
# album['abc'] = 123

# print(str(artist))
