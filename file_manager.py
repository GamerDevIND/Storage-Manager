import aiosqlite
import asyncio
import json
import re
import os
from typing import Any
from utils import Logger, File, Blob, call_async
from hashlib import sha256
from uuid import uuid7
from file_proccesser import RAW, FileProcesser

def sanitise_fts(text: str) -> str:
    tokens: list[str] = re.findall(r"[^\s]+", text.lower(), re.UNICODE)
    return ' '.join(f'"{t}"*' for t in tokens)

class DataBase:
    def __init__(self, db_name: str):
        if not db_name.endswith('.db'):
            db_name += '.db'

        self.db_path: str = os.path.abspath(db_name)
        self.db_lock = asyncio.Lock()
        self.conn = None

    async def exec(self, cmd: str, args = tuple(), get = None, sql_script: bool = False, commit_to_db: bool = True) -> Any:
        if not self.conn:
            await Logger.warn_async('Index database not initialized. Please call and await `init()`')
            return None

        async with self.db_lock:
            if sql_script:
                try:
                    await self.conn.executescript(cmd)
                    if commit_to_db:
                        await self.conn.commit()
                    return None
                except Exception as e:
                    e = repr(e)
                    await Logger.error_async(f"An error occured while executing SQL Script: {e}")
            try:
                async with self.conn.execute(cmd, tuple(args)) as cursor:
                    result = None
                    if get:
                        mode = get.strip().lower()
                        if mode in ['one', 'fetchone', 'fetch_one', 'fetch one']:
                            result = await cursor.fetchone()
                        elif mode in ['many', 'fetchmany', 'fetch_many', 'fetch many']:
                            result = await cursor.fetchmany(100)
                        elif mode in ['all', 'fetchall', 'fetch_all', 'fetch all']:
                            result = await cursor.fetchall()
                        else:
                            await Logger.error_async(f"Invalid get parameter: {get} for DB queries.")

                    if commit_to_db:
                        await self.conn.commit()

                    return result
                
            except Exception as e:
                e = repr(e)
                await Logger.error_async(f"An error occured while executing SQL command: {e}")
                return None

    async def init(self):
        self.conn = await aiosqlite.connect(self.db_path)
        await self.conn.execute("PRAGMA journal_mode=WAL;")
        await self.conn.execute("PRAGMA foreign_keys = ON;")
        self.conn.row_factory = aiosqlite.Row
        await self.create_tables()
        await Logger.success_async('Index database successfully initialized')

    async def index_file_transaction(self, file_obj: File, blobs: list[Blob]):
        if not self.conn:
            await Logger.error_async("Database not connected")
            return

        async with self.db_lock:
            file_query = '''
                    INSERT OR REPLACE INTO files (file_hash, file_name, size, metadata, index_status)
                    VALUES (?, ?, ?, ?, 'PENDING');
            '''
            meta_json = json.dumps(file_obj.metadata or {})
            await self.conn.execute(file_query, (file_obj.file_hash, file_obj.file_name, file_obj.size, meta_json))

            blob_query = '''
                    INSERT INTO blobs (blob_hash, size, compressed_size, ref_count, post_processed_hash, compression_type, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(blob_hash) DO UPDATE SET ref_count = blobs.ref_count;
            '''
            blob_tuples = [
                    (b.blob_hash, b.size, b.compressed_size, b.ref_count, b.post_processed_hash, b.compression_type, json.dumps(b.metadata or {}))
                    for b in blobs]
            await self.conn.executemany(blob_query, blob_tuples)

            link_query = '''
                INSERT OR IGNORE INTO file_blobs (file_hash, blob_hash, chunk_index, offset, size)
                VALUES (?, ?, ?, ?, ?);
            '''
            link_tuples = [
                    (file_obj.file_hash, b.blob_hash, idx, b.offset, b.size)
                    for idx, b in enumerate(blobs)
                ]
            await self.conn.executemany(link_query, link_tuples)

            await self.conn.execute(
                "UPDATE files SET index_status = 'COMPLETED' WHERE file_hash = ?",
                (file_obj.file_hash,)
            )
            await self.conn.commit()

        await Logger.success_async(f"File {file_obj.file_hash} atomically indexed.")

    async def create_tables(self):
        query = '''
            CREATE TABLE IF NOT EXISTS files(
                file_hash TEXT PRIMARY KEY,
                file_name TEXT NOT NULL,
                size INTEGER NOT NULL,
                status TEXT DEFAULT 'OK',
                metadata TEXT NOT NULL DEFAULT '{}',
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                index_status TEXT DEFAULT 'PENDING'
            );
            
            CREATE TABLE IF NOT EXISTS blobs (
                blob_hash TEXT PRIMARY KEY,
                size INTEGER NOT NULL,
                compressed_size INTEGER NOT NULL,
                ref_count INTEGER NOT NULL DEFAULT 0,
                post_processed_hash TEXT,
                compression_type INTEGER,
                status TEXT DEFAULT 'OK',
                metadata TEXT NOT NULL DEFAULT '{}',
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS file_blobs (
                file_hash TEXT NOT NULL,
                blob_hash TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                offset INTEGER NOT NULL,
                size INTEGER NOT NULL,
                PRIMARY KEY (file_hash, chunk_index),
                FOREIGN KEY (file_hash) REFERENCES files(file_hash) ON DELETE CASCADE,
                FOREIGN KEY (blob_hash) REFERENCES blobs(blob_hash) ON DELETE RESTRICT
            );

            CREATE INDEX IF NOT EXISTS idx_file_blobs_blob_hash ON file_blobs(blob_hash);
            CREATE INDEX IF NOT EXISTS idx_file_blobs_seeking ON file_blobs(file_hash, offset, size, blob_hash);

            CREATE TRIGGER IF NOT EXISTS trg_inc_blob_ref
            AFTER INSERT ON file_blobs
            BEGIN
                UPDATE blobs SET ref_count = ref_count + 1 WHERE blob_hash = NEW.blob_hash;
            END;

            CREATE TRIGGER IF NOT EXISTS trg_dec_blob_ref
            AFTER DELETE ON file_blobs
            BEGIN
                UPDATE blobs SET ref_count = ref_count - 1 WHERE blob_hash = OLD.blob_hash;
            END;
        '''.strip()

        await self.exec(query, sql_script=True)
        await Logger.success_async('Index tables successfully initialized')

    async def link_file_blob(self, file_hash: str, blob_hash: str, chunk_index: int, offset: int, size: int):
        query = '''
            INSERT OR IGNORE INTO file_blobs (file_hash, blob_hash, chunk_index, offset, size)
            VALUES (?, ?, ?, ?, ?);
        '''
        await self.exec(query, (file_hash, blob_hash, chunk_index, offset, size))

    async def index_blob(self, blob):
        if isinstance(blob, dict):
            hash_ = blob.get('hash', blob.get('blob_hash'))
            if not hash_:
                raise KeyError
            size = blob['size']
            compressed_size = blob['compressed_size']
            compression_type = blob['compression_type']
            ref_count = blob.get('ref_count', 0)
            post_processed_hash = blob.get('post_processed_hash')
            metadata = blob.get('metadata', {})
            blob_data = blob.get('data', blob.get('blob_data'))
            blob_offset = blob.get('offset', blob.get('blob_offset', 0))

            blob = Blob(hash_, size, compressed_size, compression_type, ref_count, post_processed_hash, metadata, blob_offset, blob_data)

        query = '''
            INSERT INTO blobs (blob_hash, size, compressed_size, ref_count, post_processed_hash, compression_type, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(blob_hash) DO UPDATE SET ref_count = blobs.ref_count;
        '''.strip()

        comp_type = blob.compression_type
        meta_json = json.dumps(blob.metadata or {})
        await Logger.info_async(f'Indexing Blob {blob.blob_hash}...')
        await self.exec(query, (blob.blob_hash, blob.size, blob.compressed_size, blob.ref_count, blob.post_processed_hash, comp_type, meta_json))

    async def index_file(self, file):
        if isinstance(file, dict):
            hash_ = file.get('hash', file.get('file_hash'))
            if not hash_:
                raise KeyError
            name = file.get('name', file.get('file_name')) or f"File_{str(uuid7())}"
            status = file.get('index_status', file.get('status', 'PENDING'))
            chunks = file.get('chunks', file.get('file_chunks', []))
            file_path = file.get('file_path')
            metadata = file.get('metadata', {})
            
            size = os.path.getsize(file_path) if file_path and os.path.exists(file_path) else file.get('size', 0)
            file = File(name, size, hash_, chunks, file_path, metadata, status)

        query = '''
            INSERT OR REPLACE INTO files (file_hash, file_name, size, metadata, index_status)
            VALUES (?, ?, ?, ?, ?);
        '''.strip()

        meta_json = json.dumps(file.metadata or {})
        await Logger.info_async(f'Indexing File {file.file_hash}...')
        await self.exec(query, (file.file_hash, file.file_name, file.size, meta_json, file.index_status))

    async def seek_file(self, file_name: str, starting_range: int, ending_range: int):
        query = 'SELECT file_hash, file_name, size, status, index_status, metadata, created_at FROM files WHERE file_name = ?'
        row = await self.exec(query, [file_name], get='one')
        if not row:
            await Logger.error_async(f'{file_name} does not exist in the database. Aborting...')
            return None

        file_dict = dict(row)
        file_hash = file_dict['file_hash']

        effective_ending_range = ending_range
        if starting_range == ending_range:
            effective_ending_range = starting_range + 1

        q = '''
            SELECT b.blob_hash, b.size, b.compressed_size, b.ref_count, 
                b.post_processed_hash, b.compression_type, b.status, b.metadata, b.created_at,
                fb.chunk_index as blob_chunk_idx, fb.offset as blob_offset, fb.size as blob_chunk_size
            FROM file_blobs fb
            JOIN blobs b ON fb.blob_hash = b.blob_hash
            WHERE fb.file_hash = ?
            AND fb.offset < ?
            AND (fb.offset + fb.size) > ?
            ORDER BY fb.chunk_index ASC;
        '''

        rows = await self.exec(q, [file_hash, effective_ending_range, starting_range], get='all') or []
        file_dict['blobs'] = [dict(r) for r in rows]
        return file_dict

    async def get_file(self, file_name: str):
        query = 'SELECT file_hash, file_name, size, status, index_status, metadata, created_at FROM files WHERE file_name = ?'
        row = await self.exec(query, [file_name], get='one')
        if not row:
            await Logger.error_async(f'{file_name} does not exist in the database. Aborting...')
            return None

        file_dict = dict(row)
        file_hash = file_dict['file_hash']

        query_blobs = '''
            SELECT b.blob_hash, b.size as blob_size, b.compressed_size as blob_compressed_size, 
                   b.ref_count as blob_ref_count, b.post_processed_hash as blob_post_processed_hash, 
                   b.compression_type as blob_compression_type, b.status as blob_status, 
                   b.metadata as blob_metadata, b.created_at as blob_created_at,
                   fb.chunk_index as blob_chunk_idx, fb.offset as blob_offset, fb.size as blob_chunk_size
            FROM file_blobs fb
            JOIN blobs b ON fb.blob_hash = b.blob_hash
            WHERE fb.file_hash = ?
            ORDER BY fb.chunk_index ASC
        '''

        rows = await self.exec(query_blobs, [file_hash], get='all') or []
        file_dict['blobs'] = [dict(r) for r in rows]
        return file_dict

    async def garbage_collect(self, delete_func=None):
        q = 'SELECT blob_hash FROM blobs WHERE ref_count <= 0'
        rows = await self.exec(q, get='all') or []
        await Logger.warn_async(f'Garbage collecting {len(rows)} blobs')

        for row in rows:
            blob_hash = row['blob_hash']
            await Logger.info_async(f'[GC] Garbage Collecting {blob_hash}...')
            if delete_func:
                await call_async(delete_func, blob_hash=blob_hash)
                    
            await self.exec('DELETE FROM blobs WHERE blob_hash = ?', [blob_hash])

    async def verify_blobs(self, provider_verify_func, get_blob_func=None, decompresser_func=None):
        q = 'SELECT blob_hash, size, compressed_size, post_processed_hash, status, compression_type FROM blobs'
        rows = await self.exec(q, get='all') or []

        for row in rows:
            blob_hash = row['blob_hash']
            size = row['size']
            compressed_size = row['compressed_size']
            post_processed_hash = row['post_processed_hash']
            status = row['status']
            compression_type = row['compression_type']

            verified = await call_async(
                provider_verify_func, 
                blob_hash=blob_hash, 
                size=compressed_size, 
                status=status, 
                stored_blob_hash=post_processed_hash, 
                delete_bad=False, 
                read_verify=True
            )

            if verified and get_blob_func:
                blob_data = await call_async(get_blob_func, blob_hash=blob_hash)

                if len(blob_data) != compressed_size:
                    await Logger.warn_async(f'{blob_hash} size mismatch ({compressed_size} indexed vs {len(blob_data)} got)')
                    verified = False
                elif decompresser_func:
                    data = FileProcesser.CompressedBlob(blob_data, compression_type)
                    d = await call_async(decompresser_func, data=data)

                    if len(d) != size:
                        await Logger.warn_async(f'{blob_hash} decompressed size mismatch ({size} indexed vs {len(d)} got)')
                        verified = False
                    elif sha256(d).hexdigest() != blob_hash:
                        await Logger.warn_async(f'{blob_hash} hash validation failed.')
                        verified = False

            if not verified:
                await Logger.warn_async(f'{blob_hash} is BAD. Updating index status...')
                await self.exec("UPDATE blobs SET status = 'BAD' WHERE blob_hash = ?", [blob_hash])

        await Logger.success_async('Verification of all indexed blobs successfully completed.')

    async def verify_files(self, get_blob_func, decompresser_func):
        if not get_blob_func or not decompresser_func:
            await Logger.error_async('`get_blob_func` and `decompresser_func` are required for file verification.')
            return

        rows = await self.exec('SELECT file_hash, size FROM files', get='all') or []
        await Logger.info_async(f'Verifying {len(rows)} file(s)')

        for file in rows:
            file_hash = file['file_hash']
            file_size = file['size']
            
            q = 'SELECT blob_hash FROM file_blobs WHERE file_hash = ? ORDER BY chunk_index ASC'
            blobs = await self.exec(q, [file_hash], get='all') or []
            
            file_hasher = sha256()
            total_bytes = 0

            for blob in blobs:
                blob_hash = blob['blob_hash']
                blob_data = await call_async(get_blob_func, blob_hash=blob_hash)
                
                row = await self.exec('SELECT compression_type FROM blobs WHERE blob_hash = ?', [blob_hash], get='one')
                compression_type = row['compression_type'] if row else RAW

                data = FileProcesser.CompressedBlob(blob_data, compression_type)
                decompressed_data = await call_async(decompresser_func, data=data)

                total_bytes += len(decompressed_data)
                file_hasher.update(decompressed_data)

            if total_bytes != file_size:
                await Logger.error_async(f'File {file_hash} size mismatch ({file_size} expected vs {total_bytes} read)')
                await self.exec("UPDATE files SET status = 'BAD' WHERE file_hash = ?", [file_hash])
                continue

            if file_hasher.hexdigest() != file_hash:
                await Logger.error_async(f'File {file_hash} hash integrity check failed')
                await self.exec("UPDATE files SET status = 'BAD' WHERE file_hash = ?", [file_hash])
                continue

        await Logger.success_async('All files verified.')

    async def verify(self, provider_verify_func, get_blob_func=None, decompresser_func=None):
        await self.verify_blobs(provider_verify_func, get_blob_func, decompresser_func)
        await self.verify_files(get_blob_func, decompresser_func)

    async def close(self):
        if not self.conn:
            await Logger.warn_async('Index database not initialized. Cannot close.')
            return
        
        async with self.db_lock:
            await self.conn.commit()
            await self.conn.close()
            self.conn = None
        await Logger.success_async('Index database successfully closed')