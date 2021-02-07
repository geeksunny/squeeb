from __future__ import annotations
import logging
import sqlite3
from contextlib import closing
from typing import Any, List, Tuple, Union

logger = logging.getLogger()


class _Model(dict):

    def __init__(self, taglib_song=None, sqlite_row=None, sqlite_field_mapping=None) -> None:
        super().__init__()
        if taglib_song is not None:
            self.populate(taglib_song)
        if sqlite_row is not None:
            self.from_sqlite(sqlite_row, sqlite_field_mapping)

    def _set_if_tag_exists(self, field_name, source, source_field=None) -> None:
        if source_field is None:
            source_field = field_name
        if source[source_field] is not None:
            self[field_name] = source[source_field][0] if isinstance(source[source_field], list)\
                else source[source_field]

    def populate(self, taglib_song) -> None:
        raise NotImplementedError()

    def from_sqlite(self, row: sqlite3.Row, sqlite_field_mapping=None) -> None:
        for sql_key in row.keys():
            key = sqlite_field_mapping[sql_key]\
                if sqlite_field_mapping is not None and sql_key in sqlite_field_mapping\
                else sql_key
            self[key] = row[sql_key]


class Track(_Model):

    def populate(self, taglib_song) -> None:
        self['filepath'] = taglib_song.path
        self['duration'] = taglib_song.length
        self._set_if_tag_exists('name', taglib_song, 'TITLE')
        self._set_if_tag_exists('album_track_number', taglib_song, 'TRACKNUMBER')

    def set_artist(self, artist):
        if 'id' in artist:
            self['artist_id'] = artist['id']

    def set_album_artist(self, artist):
        if 'id' in artist:
            self['album_artist_id'] = artist['id']

    def set_album(self, album):
        if 'id' in album:
            self['album_id'] = album['id']


class Artist(_Model):

    def populate(self, taglib_song) -> None:
        self._set_if_tag_exists('name', taglib_song, 'ARTIST')
        self._set_if_tag_exists('album_artist', taglib_song, 'ALBUMARTIST')

    def needs_split(self) -> bool:
        return 'album_artist' in self

    def split(self) -> Artist:
        album_artist = Artist()
        album_artist['name'] = self['album_artist']
        del self['album_artist']
        return album_artist


class Album(_Model):

    def populate(self, taglib_song) -> None:
        # todo: genres field... array of all genres in tracks?
        self._set_if_tag_exists('name', taglib_song, 'ALBUM')
        self._set_if_tag_exists('album_artist', taglib_song, 'ALBUMARTIST')

    def set_artist(self, artist):
        if 'id' in artist:
            self['artist_id'] = artist['id']


class _DbHandler(object):

    _conn = None

    def __init__(self) -> None:
        super().__init__()
        self._conn = sqlite3.connect(self._db_filename())
        self._conn.row_factory = sqlite3.Row

    def _db_filename(self) -> str:
        raise NotImplementedError()

    def _init_tables(self) -> bool:
        raise NotImplementedError()

    def __del__(self):
        self.close()

    def _exec_raw_query_no_result(self, query_str: str, args = None) -> bool:
        try:
            with closing(self._conn.cursor()) as c:
                c.execute(query_str, args)
                return True
        except sqlite3.Error as e:
            logger.error(e)
            return False

    def _exec_raw_query_single_result(self, query_str: str, args = None) -> sqlite3.Row:
        try:
            with closing(self._conn.cursor()) as c:
                c.execute(query_str, args)
                return c.fetchone()
        except sqlite3.Error as e:
            logger.error(e)
            return None

    def _exec_raw_query_all_results(self, query_str: str, args: Tuple[Any] = None) -> Union[List[sqlite3.Row], None]:
        try:
            with closing(self._conn.cursor()) as c:
                c.execute(query_str, args)
                return c.fetchall()
        except sqlite3.Error as e:
            logger.error(e)
            return None

    def _exec_query_no_result(self, query_obj) -> bool:
        query = query_obj.build()
        if 'error' in query:
            logger.error('QUERY BUILD ERROR: %s', query['error'])
            return False
        return self._exec_raw_query_no_result(query['query'], query['args'])

    def _exec_query_single_result(self, query_obj) -> Union[sqlite3.Row, None]:
        query = query_obj.build()
        if 'error' in query:
            logger.error('QUERY BUILD ERROR: %s', query['error'])
            return None
        return self._exec_raw_query_single_result(query['query'], query['args'])

    def _exec_query_all_results(self, query_obj) -> Union[List[sqlite3.Row], None]:
        query = query_obj.build()
        if 'error' in query:
            logger.error('QUERY BUILD ERROR: %s', query['error'])
            return None
        return self._exec_query_raw_all_results(query['query'], query['args'])

    def _table_exists(self, table_name) -> bool:
        return self._exec_raw_query_single_result(
            "SELECT * FROM sqlite_master WHERE type='table' AND name='%s'" % table_name)[0] == 1

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


class _MusicDb(_DbHandler):

    # todo: crud methods for music data

    def _db_filename(self) -> str:
        return "smrtypntz.db"

    def _init_tables(self) -> bool:
        success = True
        if self._table_exists("artists") is False:
            success = success and self._create_table_artists()
        if self._table_exists("albums") is False:
            success = success and self._create_table_albums()
        if self._table_exists("tracks") is False:
            success = success and self._create_table_tracks()
        return success

    def _create_table_artists(self) -> bool:
        return self._exec_raw_query_no_result('''CREATE TABLE "artists" (
                "id" INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
                "spotify_id" TEXT,
                "name" TEXT,
                "spotify_genres" TEXT,
                "spotify_popularity" INTEGER
            )''') and self._exec_raw_query_no_result('''CREATE UNIQUE INDEX "artist_names" ON "artists" ("name");''')

    def _create_table_albums(self) -> bool:
        return self._exec_raw_query_no_result('''CREATE TABLE "albums" (
                    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
                    "spotify_id" TEXT,
                    "name" TEXT,
                    "year" INTEGER,
                    "genres" TEXT,
                    "artist_id" INTEGER REFERENCES "artists"("id"),
                    "spotify_genres" TEXT
                )''') and self._exec_raw_query_no_result('''CREATE UNIQUE INDEX "artist-name-year"
                                                  ON "albums" ("name","year","artist_id");''')

    def _create_table_tracks(self) -> bool:
        return self._exec_raw_query_no_result('''CREATE TABLE "tracks" (
                "id" INTEGER PRIMARY KEY AUTOINCREMENT,
                "spotify_id" TEXT,
                "filepath" TEXT,
                "name" TEXT,
                "duration" INTEGER,
                "artist_id" INTEGER REFERENCES "artists"("id"),
                "album_artist_id" INTEGER REFERENCES "artists"("id"),
                "album_id" INTEGER REFERENCES "albums"("id"),
                "album_track_number" INTEGER,
                "danceability" REAL,
                "energy" REAL,
                "instrumentalness" REAL,
                "key" INTEGER,
                "liveness" REAL,
                "loudness" REAL,
                "mode" INTEGER,
                "speechiness" REAL,
                "tempo" REAL,
                "time_signature" INTEGER,
                "valence" REAL
            )''')


# Singleton instance of the db handler
db = _MusicDb()
