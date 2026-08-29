from typing import Any, Callable
from utils import Logger, Blob, call_async
import asyncio
import os
import aiosqlite
import json
import aiofiles
from hashlib import sha256
from pathlib import Path

class LocalProvider:
    def __init__(self, main_dir = './storage') -> None:
        self.main_dir = main_dir

        os.makedirs(main_dir, exist_ok=True)
        db_path: str = os.path.abspath(os.path.join(main_dir, 'blobs.db'))
        self.db_path = db_path
        self.name = 'Local Storage'
        self.blobs_dir = os.path.abspath(os.path.join(main_dir, 'blobs/'))
        os.makedirs(self.blobs_dir, exist_ok=True)
        self.db_lock = asyncio.Lock()
        self.conn = None

    async def exec(self, cmd: str, args:list | tuple = tuple(), get: None | str = None, sql_script: bool = False, commit_to_db: bool = True) -> Any:
            args = tuple(args)
            if not self.conn:
                await Logger.warn_async(f'[DB ({self.name})] Index database not initalised please do so by calling and awaiting `init()`')
                return
            
            async with self.db_lock:
                cursor = await self.conn.cursor()
                if sql_script:
                    await cursor.executescript(cmd)
                else:
                    await cursor.execute(cmd, tuple(args))
    
            if commit_to_db:
                await self.conn.commit()
    
            if get:
                get = get.strip().lower() 
                if get == 'cursor':
                    return cursor
                elif get in ['one', 'fetchone', 'fetch_one', 'fetch one']:
                    return await cursor.fetchone()
                elif get in ['many', 'fetchmany', 'fetch_many', 'fetch many']:
                    return await cursor.fetchmany()
                elif get in ['all', 'fetchall', 'fetch_all', 'fetch all']:
                    return await cursor.fetchall()
                else:
                    await Logger.error_async(f"[DB ({self.name})] Invalid get parameter: {get} for DB queries.")
        
    async def init(self,):
        self.conn = await aiosqlite.connect(self.db_path)
        await self.conn.execute("PRAGMA journal_mode=WAL;")
        await self.create_tables()
        await Logger.success_async(f'[DB ({self.name})] Index database successfully initilaised')

    async def verify_blob(self, blob_hash, size, status, delete_bad, read_verify, stored_blob_hash):
        q = 'SELECT file_path from blobs WHERE blob_hash = ?'
        if not self.conn:
            await Logger.warn_async(f'[DB ({self.name})] Index database not initalised please do so by calling and awaiting `init()`')
            return False

        row = await self.exec(q, [blob_hash], get='one')
        if not row or not row[0]:
            return False

        file_path = row[0]
        if not await asyncio.to_thread(os.path.exists, file_path):
            await Logger.warn_async(f'[DB ({self.name})] {blob_hash} on {file_path} does not exist on disk, removing from index.')
            await self.exec('DELETE FROM blobs WHERE blob_hash = ?', [blob_hash])
            return False

        disk_size = await asyncio.to_thread(os.path.getsize, file_path)
        if disk_size != size:
            await Logger.warn_async(f'[DB ({self.name})] {blob_hash} on {file_path}; The indexed file size ({size} bytes) does not match the disk stored size ({disk_size} bytes).')
            if delete_bad:
                await Logger.warn_async(f'[DB ({self.name})] deleting {file_path}...')
                await self.delete_blob(blob_hash, True)
            else:
                await self.exec("UPDATE blobs SET status = 'CORRUPTED' WHERE blob_hash = ?", [blob_hash])
            return False
        
        if not str(status).lower() == 'ok':
            await Logger.warn_async(f'[DB ({self.name})] the status of {blob_hash} on {file_path} is not "OK"')
            if delete_bad:
                await Logger.warn_async(f'[DB ({self.name})] deleting {file_path}...')
                await self.delete_blob(blob_hash, True)
            else:
                q = "UPDATE blobs SET status = 'CORRUPTED' WHERE blob_hash = ?"
                await self.exec(q, [blob_hash])
            return False
        
        if read_verify:
            blob_data = await self.get_blob(blob_hash)
            if not blob_data:
                await Logger.warn_async(f'[DB ({self.name})] {blob_hash} on {file_path} does not have any data.')
                if delete_bad:
                    await Logger.warn_async(f'[DB ({self.name})] deleting {file_path}...')
                    await self.delete_blob(blob_hash, True)
                else:
                    q = "UPDATE blobs SET status = 'CORRUPTED' WHERE blob_hash = ?"
                    await self.exec(q, [blob_hash])

                return False
        
            hashed = sha256(blob_data).hexdigest()
            if hashed != stored_blob_hash:
                await Logger.warn_async(f'[DB ({self.name})] {blob_hash} on {file_path} the hashed data from disk does not match the stored hash')
                if delete_bad:
                    await Logger.warn_async(f'[DB ({self.name})] deleting {file_path}...')
                    await self.delete_blob(blob_hash, True)
                else:
                    q = "UPDATE blobs SET status = 'CORRUPTED' WHERE blob_hash = ?"
                    await self.exec(q, [blob_hash])

                return False

        return True

    async def verify(self, read_verify = False, delete_bad = True):
        q = "SELECT blob_hash, stored_blob_hash, size, status from blobs"
        await self.exec(q)
        if not self.conn:
            await Logger.warn_async(f'[DB ({self.name})] Index database not initalised please do so by calling and awaiting `init()`')
            return
        
        rows = list(await self.exec(q, get='all'))
        total_blobs = len(rows)
        for i, row in enumerate(rows, 1):
            await Logger.info_async(f'[DB ({self.name})] Verifying {i} / {total_blobs} blob...')
            row = tuple(row)
            blob_hash, stored_blob_hash, size, status = row

            await self.verify_blob(blob_hash=blob_hash, size=size, status=status, 
                             stored_blob_hash=stored_blob_hash, delete_bad=delete_bad, read_verify=read_verify)

        await Logger.success_async(f'[DB ({self.name})] Verification completed.')

    async def close(self):
        if not self.conn:
            await Logger.warn_async(f'[DB ({self.name})] Index database not initalised please do so by calling and awaiting `init()`')
            return
        
        async with self.db_lock:
            await self.conn.commit()
            await self.conn.close()
        await Logger.success_async(f'[DB ({self.name})] Index database successfully closed')

    async def create_tables(self):
        query = '''
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS blobs(
                blob_hash TEXT NOT NULL PRIMARY KEY,
                stored_blob_hash TEXT NOT NULL UNIQUE,
                size INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                status TEXT DEFAULT 'OK',
                metadata TEXT NOT NULL DEFAULT '{}',
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        '''.strip()

        await self.exec(query, sql_script=True)
        await Logger.success_async(f'[DB ({self.name})] Index tables successfully initilaised')

    async def put_blob(self, blob_hash: str | Blob, post_processed_hash: str | Blob, data: bytes | Blob, metadata: dict | None = None, writer: None | Callable = None):
        if isinstance(blob_hash, Blob):
            blob_hash = blob_hash.blob_hash

        if isinstance(post_processed_hash, Blob):
            post_processed_hash = post_processed_hash.post_processed_hash or ""

        blob_dir = os.path.join(self.blobs_dir, blob_hash[:2])
        os.makedirs(blob_dir, exist_ok=True)
        file_path = os.path.abspath(os.path.join(blob_dir, blob_hash))

        if isinstance(data, Blob):
            data = data.data or bytes()

        async def handle_exists(file_path):
            async with aiofiles.open(file_path, 'rb') as f:
                r = await f.read()
            hasher = sha256()
            await asyncio.to_thread(hasher.update, r)
            return hasher.hexdigest()

        if await asyncio.to_thread(os.path.exists, file_path):
            r = await handle_exists(file_path)
            if r:
                q = "SELECT stored_blob_hash FROM blobs WHERE blob_hash = ?"
                if not self.conn:
                    await Logger.warn_async(f'[DB ({self.name})] Index database or cursor not initalised please do so by calling and awaiting `init()`')
                else:
                    row = await self.exec(q, (blob_hash,), get='one')
                    if row and row[0]:
                        if row[0] == r:
                            await Logger.info_async(f'[DB ({self.name})] {blob_hash} already exists on: "{file_path}"; Skipping write...')
                            return file_path

        async def get_default_writer(data, file_path):
            if not data:
                await Logger.warn_async(f'[DB ({self.name})] No data provided for "{file_path}"; aborting')
                return
            async with aiofiles.open(file_path, 'wb') as f:
                r = await f.write(data)
                await f.flush()

        writer = writer or get_default_writer

        await Logger.info_async(f'[DB ({self.name})] Attempting to save blob: {blob_hash}')

        try:
            w = await call_async(writer(data = data, file_path = file_path))

            status = 'ok'
        except Exception as e:
            await Logger.error_async(f'[DB ({self.name})] an error occured trying to save blob {blob_hash}: {repr(e)}')
            status = 'corrupted'

        query = '''
            INSERT INTO blobs (blob_hash, stored_blob_hash, size, file_path, status, metadata)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(blob_hash) DO UPDATE SET stored_blob_hash = excluded.stored_blob_hash, file_path = excluded.file_path, 
            status = excluded.status, metadata = excluded.metadata;
        '''
        await self.exec(query, (blob_hash, post_processed_hash, len(data), file_path, status.upper(), json.dumps(metadata or {})))
        return file_path

    async def get_blob(self, blob_hash: str | Blob):
        if not self.conn:
            await Logger.warn_async(f'[DB ({self.name})] Index database or cursor not initalised please do so by calling and awaiting `init()`')
            return
        if isinstance(blob_hash, Blob):
            blob_hash = blob_hash.blob_hash

        await Logger.info_async(f'[DB ({self.name})] Attempting to retrieve blob: {blob_hash}')
        row = await self.exec("SELECT file_path FROM blobs WHERE blob_hash = ?", (blob_hash,), get='one')

        if not row or not os.path.exists(row[0]):
            await Logger.warn_async(f'[DB ({self.name})] {blob_hash} does not exist in the file system. Attempting recovery...')
            blob_dir = os.path.join(self.blobs_dir, blob_hash[:2])
            file_path = os.path.abspath(os.path.join(blob_dir, blob_hash))
            if await asyncio.to_thread(os.path.exists, file_path):
                try:
                    async with aiofiles.open(file_path, 'rb') as f:
                        fb = await f.read()
                        pp_hash = sha256(fb).hexdigest()
                        await self.put_blob(blob_hash, pp_hash, fb)
                        await Logger.success_async(f'[DB ({self.name})] {blob_hash} recovered')
                        return fb
                except Exception as e:
                    await Logger.error_async(f'[DB ({self.name})] an error occured trying to read blob: {blob_hash}: {repr(e)}')

            await Logger.success_async(f'[DB ({self.name})] {blob_hash} recovery failed. Aborting...')
            return

        try:
            async with aiofiles.open(row[0], 'rb') as f:
                return await f.read()
        except Exception as e:
            await Logger.error_async(f'[DB ({self.name})] an error occured trying to read blob: {blob_hash}: {repr(e)}')
            return

    async def delete_blob(self, blob_hash: str | Blob, missing_ok = False):
        if not self.conn:
            await Logger.warn_async(f'[DB ({self.name})] Index database or cursor not initalised please do so by calling and awaiting `init()`')
            return
        
        if isinstance(blob_hash, Blob):
            blob_hash = blob_hash.blob_hash

        q = '''
            SELECT file_path FROM blobs WHERE blob_hash = ?
        '''.strip()

        row = await self.exec(q, [blob_hash], get='one')

        if not row or not os.path.exists(row[0]):
            await Logger.error_async(f'[DB ({self.name})] {blob_hash} does not exist in the file system. Aborting...')
            return

        file_path = row[0]

        try:
            await asyncio.to_thread(lambda: Path(file_path).unlink(missing_ok=missing_ok))
            await Logger.info_async(f'[DB ({self.name})] {blob_hash} deleted successfully.')
        except Exception as e:
            e = repr(e)
            await Logger.error_async(f'[DB ({self.name})] An error occured while deleting {blob_hash}: {e}')

        if not await asyncio.to_thread(os.path.exists, file_path):
            q = 'DELETE FROM blobs WHERE blob_hash = ?'
            await self.exec(q, [blob_hash])
        else:
            r = range(5)
            total_attempts = len(r)
            await Logger.warn_async(f'[DB ({self.name})] {file_path} did not get deleted. Retrying {total_attempts} times...')
            for i in r:
                await Logger.info_async(f'[DB ({self.name})] Attempt {i + 1} / {total_attempts}')

                try:
                    await asyncio.to_thread(lambda: Path(file_path).unlink(missing_ok=missing_ok))
                except Exception as e:
                    e = repr(e)
                    await Logger.error_async(f'[DB ({self.name})] An error occured while deleting {blob_hash}: {e}')

                if not await asyncio.to_thread(os.path.exists, file_path):
                    await Logger.success_async(f'[DB ({self.name})] Deletion successful.')
                    break 
                else:
                    await Logger.warn_async(f'[DB ({self.name})] Failed to delete {file_path}')